/* 
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System.Net;
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;

namespace Aerospike.Test
{
	/// <summary>
	/// Client-side translation against the unresolved-access profile, whose nodes configure no
	/// access address. Each node advertises only its private service address, and the client maps
	/// that address to the node's Toxiproxy listener with <see cref="PortMapAddressTranslator"/>.
	/// Nothing on the server depends on resolving a hostname, so this holds on servers that resolve
	/// access addresses at startup and on servers that advertise them verbatim (TG-18).
	/// </summary>
	[TestClass]
	public class TestL2PortMapNoAccessAddress
	{
		private const string BLOCKER = "Port-map without an access address needs DISCOVERY_UNRESOLVED_HOSTNAME, " +
			"nodes configuring no access address (L2_PROFILE=unresolved-access)";
		private const string SET = "l2-no-access";
		private const int KEY_COUNT = 300;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		/// <summary>
		/// Every node advertises no alternate address and a standard address the client cannot reach.
		/// With a port-map from each standard address to its listener, the client holds one distinct
		/// <see cref="Node"/> per server node at that listener, every partition has an active master,
		/// and writes and reads with replica MASTER are served by each partition's master without a
		/// server proxy hop.
		/// </summary>
		[TestMethod]
		public void PortMapToListenersRoutesEveryPartition()
		{
			TestL2Topology.SkipUnless(L2Topology.UnresolvedHostname.Length > 0, BLOCKER);

			List<L2Node> nodes = L2Topology.Nodes();
			Dictionary<string, string> portMap = L2Topology.PortMapByEndpoint(nodes);
			Console.WriteLine("L2-NO-ACCESS-PORT-MAP " + TestL2Topology.FmtMap(portMap));

			foreach (L2Node node in nodes)
			{
				List<string> alternates = L2Topology.Alternates(node);
				Console.WriteLine("L2-NO-ACCESS-NODE " + node + " alternate=" + TestL2Topology.Fmt(alternates));
				CollectionAssert.AreEqual(new List<string>(), alternates, node.Name + " alternate list");
				L2Topology.AssertUnreachable(node.Advertised[0]);
			}

			List<string> addresses = new List<string>();

			foreach (IPAddress address in Dns.GetHostAddresses(L2Topology.Host))
			{
				addresses.Add(address.ToString());
			}

			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(portMap);

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned("no-access-port-map", client);
				HashSet<Node> distinct = new HashSet<Node>(ReferenceEqualityComparer.Instance);
				foreach (Node master in masters)
				{
					distinct.Add(master);
				}
				Dictionary<string, int> owners = L2Topology.Owners(masters);
				Console.WriteLine("L2-NO-ACCESS-OWNERS nodes=" + distinct.Count + " partitionsPerNode=" +
					TestL2Topology.FmtMap(owners));
				Assert.AreEqual(nodes.Count, distinct.Count);
				CollectionAssert.AreEqual(L2Topology.NodeNames(), TestL2Topology.Sorted(owners.Keys));

				foreach (Node node in client.Nodes)
				{
					L2Node l2 = NodeOf(nodes, node.Name);
					Host h = node.Host;
					Console.WriteLine("L2-NO-ACCESS-HOST " + node.Name + " host=" + h.name + ':' + h.port);
					Assert.AreSame(node, client.Cluster.GetNode(node.Name), node.Name);
					Assert.IsTrue(h.name.Equals(L2Topology.Host) || addresses.Contains(h.name),
						h.name + " is not " + L2Topology.Host + " " + TestL2Topology.Fmt(addresses));
					Assert.AreEqual(l2.Port, h.port);
				}
				AssertServedByOwners(client, masters, nodes);
			}
		}

		private static void AssertServedByOwners(AerospikeClient client, Node[] masters, List<L2Node> nodes)
		{
			string[] stats = {"client_write_success", "client_read_success", "client_proxy_complete",
				"client_proxy_error", "client_proxy_timeout"};
			Dictionary<string, Dictionary<string, long>> before = new Dictionary<string, Dictionary<string, long>>();

			foreach (L2Node node in nodes)
			{
				before[node.Name] = L2Topology.NamespaceStats(node, stats);
			}

			Cluster cluster = client.Cluster;
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			wp.replica = Replica.MASTER;
			Policy rp = new Policy(client.ReadPolicyDefault);
			rp.replica = Replica.MASTER;
			Dictionary<string, long> served = new Dictionary<string, long>();

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(L2Topology.Namespace, SET, "no-access-" + i);
				Node owner = masters[(int)Partition.GetPartitionId(key.digest)];
				Assert.AreSame(owner, Partition.Write(cluster, wp, key).GetNodeWrite(cluster));
				Assert.AreSame(owner, Partition.Read(cluster, rp, key).GetNodeRead(cluster));
				client.Put(wp, key, new Bin("v", i));
				Assert.AreEqual(i, client.Get(rp, key).GetInt("v"));
				served[owner.Name] = served.GetValueOrDefault(owner.Name, 0L) + 1;
			}

			foreach (L2Node node in nodes)
			{
				Dictionary<string, long> after = L2Topology.NamespaceStats(node, stats);
				Dictionary<string, long> start = before[node.Name];
				long writes = after["client_write_success"] - start["client_write_success"];
				long reads = after["client_read_success"] - start["client_read_success"];
				long proxied = after["client_proxy_complete"] - start["client_proxy_complete"] +
					after["client_proxy_error"] - start["client_proxy_error"] +
					after["client_proxy_timeout"] - start["client_proxy_timeout"];
				long expected = served.GetValueOrDefault(node.Name, 0L);
				Console.WriteLine("L2-NO-ACCESS-SERVED node=" + node.Name + " keys=" + expected + " writes=" + writes +
					" reads=" + reads + " proxied=" + proxied);
				Assert.IsTrue(expected > 0, node.Name + " served no keys");
				Assert.AreEqual(expected, writes, node.Name + " writes");
				Assert.AreEqual(expected, reads, node.Name + " reads");
				Assert.AreEqual(0L, proxied, node.Name + " proxied");
			}
		}

		private static L2Node NodeOf(List<L2Node> nodes, string name)
		{
			foreach (L2Node node in nodes)
			{
				if (node.Name.Equals(name))
				{
					return node;
				}
			}
			throw new InvalidOperationException("Unknown node " + name);
		}
	}
}
