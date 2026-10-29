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
	/// Testing strategy section 4.3 row N-10 and the alternate list cases against the reachable profile
	/// of test/discovery/run_l2.sh. The node named by DISCOVERY_REACHABLE_NODE advertises a hostname and
	/// its own listener port as its access address; the hostname resolves to the proxy in the client
	/// and server containers. Every other node advertises its private address in the standard list and
	/// the same hostname with its own listener port in the alternate list. Without the profile every
	/// case is skipped.
	/// </summary>
	[TestClass]
	public class TestL2ReachableNode
	{
		private const string BLOCKER = " needs DISCOVERY_REACHABLE_NODE, a node advertising an address the " +
			"client can reach (run_l2.sh L2_PROFILE=reachable, or the discovery-l2 workflow's reachable profile)";
		private const int MISS_RETRIES = 3;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);
		}

		/// <summary>
		/// N-10: on_miss unset (pass_through). port-map covers every node but the reachable one, whose
		/// advertised address is used unchanged for its <see cref="Node"/>. All nodes join, no translation
		/// miss is logged and the partition map is complete.
		/// </summary>
		[TestMethod]
		public void N10UnmappedPeerPassesThroughUnchanged()
		{
			TestL2Topology.SkipUnless(L2Topology.ReachableNode.Length > 0, "N-10" + BLOCKER);

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node reachable = Reachable(nodes);
			List<L2Node> mapped = new List<L2Node>(nodes);
			mapped.Remove(reachable);
			string advertised = reachable.Advertised[0];
			Dictionary<string, string> portMap = L2Topology.PortMapByEndpoint(mapped);

			Endpoint endpoint = new Endpoint(reachable.Name, null, HostOf(advertised),
				int.Parse(advertised.Substring(advertised.LastIndexOf(':') + 1)), Endpoint.SourceList.STANDARD);
			Assert.IsNull(new PortMapAddressTranslator(portMap).Translate(endpoint), advertised + " is mapped");
			Assert.AreEqual(TranslationMissMode.PASS_THROUGH, new ClientPolicy().translationOnMiss);
			Console.WriteLine("L2-N10-ADVERTISED node=" + reachable.Name + " container=" + reachable.Proxy +
				" advertised=" + TestL2Topology.Fmt(reachable.Advertised) + " portMap=" + TestL2Topology.FmtMap(portMap));

			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(portMap);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, mapped[0].Port)))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());

				Node node = client.Cluster.GetNode(reachable.Name);
				Host h = node.Host;
				Console.WriteLine("L2-N10-NODE " + node.Name + " host=" + h.name + ':' + h.port + " active=" + node.Active +
					" advertised=" + advertised + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)));
				Assert.IsTrue(node.Active);
				AssertAddress(advertised, h);

				Dictionary<string, int> owners = L2Topology.Owners(L2Topology.AssertAllPartitionsOwned("N-10", client));
				CollectionAssert.AreEqual(L2Topology.NodeNames(), TestL2Topology.Sorted(owners.Keys));
				Assert.AreEqual(0, TranslationMisses().Count, TestL2Topology.Fmt(TranslationMisses()));
			}
		}

		/// <summary>
		/// useServicesAlternate with the default translator: the nodes with an alternate address join
		/// on it with no translation miss, and the reachable node, which has none, is not added and is
		/// reported on every tend.
		/// </summary>
		[TestMethod]
		public void AlternateDefaultJoinsNodesWithAlternate()
		{
			RequireAlternates();
			AssertAlternateJoins("alternate-default", AlternatePolicy());
		}

		/// <summary>
		/// useServicesAlternate, on_miss strict and an ipMap that maps the alternate hostname to itself:
		/// every alternate address is mapped, so the nodes with one join.
		/// </summary>
		[TestMethod]
		public void AlternateIdentityIpMapStrictJoins()
		{
			RequireAlternates();

			string hostname = HostOf(Alternates(L2Topology.Nodes()[0])[0]);
			ClientPolicy policy = AlternatePolicy();
			policy.ipMap = new Dictionary<string, string> {[hostname] = hostname};
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertAlternateJoins("alternate-ipmap-identity-strict", policy);
		}

		/// <summary>
		/// useServicesAlternate, on_miss pass_through and a custom translator that maps nothing: every
		/// alternate address is used unchanged.
		/// </summary>
		[TestMethod]
		public void AlternateEmptyCustomPassThroughJoins()
		{
			RequireAlternates();

			ClientPolicy policy = AlternatePolicy();
			policy.addressTranslator = new TestL2Topology.EmptyTranslator();
			policy.translationOnMiss = TranslationMissMode.PASS_THROUGH;
			AssertAlternateJoins("alternate-custom-empty-pass-through", policy);
		}

		/// <summary>
		/// useServicesAlternate, on_miss strict and an empty ipMap: the peer with an alternate address
		/// misses on the alternate list on every tend and only the seed is a <see cref="Node"/>.
		/// </summary>
		[TestMethod]
		public void AlternateEmptyIpMapStrictDropsPeer()
		{
			RequireAlternates();

			ClientPolicy policy = AlternatePolicy();
			policy.ipMap = new Dictionary<string, string>();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertAlternateMisses("alternate-ipmap-empty-strict", policy, "static-map");
		}

		/// <summary>
		/// useServicesAlternate, on_miss strict and a custom translator that maps nothing: as with the
		/// empty ipMap, named by the translator class.
		/// </summary>
		[TestMethod]
		public void AlternateEmptyCustomStrictDropsPeer()
		{
			RequireAlternates();

			ClientPolicy policy = AlternatePolicy();
			policy.addressTranslator = new TestL2Topology.EmptyTranslator();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertAlternateMisses("alternate-custom-empty-strict", policy, typeof(TestL2Topology.EmptyTranslator).FullName);
		}

		/// <summary>
		/// useServicesAlternate seeded at the node without an alternate address: initialization fails
		/// with an error that names the node and the flag.
		/// </summary>
		[TestMethod]
		public void AlternateSeedWithoutAlternateFailsInit()
		{
			RequireAlternates();

			L2Node reachable = Reachable(L2Topology.Nodes());
			AerospikeException.AlternateAddressMissing e = null;

			try
			{
				new AerospikeClient(AlternatePolicy(), new Host(L2Topology.Host, reachable.Port)).Close();
			}
			catch (AerospikeException.AlternateAddressMissing ex)
			{
				e = ex;
			}

			Assert.IsNotNull(e, "Expected AerospikeException.AlternateAddressMissing");
			Console.WriteLine("L2-ALT-SEED-MISSING " + e.GetType().FullName + ": " + e.Message);
			Assert.IsTrue(e.Message.Contains(reachable.Name), e.Message);
			Assert.IsTrue(e.Message.Contains("useServicesAlternate"), e.Message);
		}

		private static void AssertAlternateJoins(string row, ClientPolicy policy)
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node reachable = Reachable(nodes);
			List<L2Node> withAlternate = WithAlternate(nodes);
			List<string> names = Names(withAlternate);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, withAlternate[0].Port)))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
					return AlternateMissing(reachable).Count >= MISS_RETRIES;
				});

				List<string> missing = AlternateMissing(reachable);
				Console.WriteLine("L2-ALT-JOIN " + row + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " withoutAlternate=" +
					reachable.Name + " missingErrors=" + missing.Count + " translationMisses=" + TranslationMisses().Count);

				foreach (L2Node expected in withAlternate)
				{
					Host h = client.Cluster.GetNode(expected.Name).Host;
					Console.WriteLine("L2-ALT-NODE " + row + ' ' + expected.Name + " host=" + h.name + ':' + h.port +
						" alternate=" + TestL2Topology.Fmt(Alternates(expected)));
					AssertAddress(Alternates(expected)[0], h);
				}

				CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
				Assert.IsTrue(missing.Count >= MISS_RETRIES,
					"Node " + reachable.Name + " without alternate not reported every tend: " + TestL2Topology.Fmt(TestL2Topology.Errors));
				Console.WriteLine("L2-ALT-LOG " + row + ' ' + missing[0]);
				Assert.AreEqual(0, TranslationMisses().Count, TestL2Topology.Fmt(TranslationMisses()));
			}
		}

		private static void AssertAlternateMisses(string row, ClientPolicy policy, string translator)
		{
			List<L2Node> withAlternate = WithAlternate(L2Topology.Nodes());
			L2Node seed = withAlternate[0];
			L2Node peer = withAlternate[1];
			string endpoint = Alternates(peer)[0];
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, seed.Port)))
			{
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(new List<string> {seed.Name}, L2Topology.NodeNames(client));
					return TestL2Topology.Misses(peer).Count >= MISS_RETRIES;
				});

				List<string> misses = TestL2Topology.Misses(peer);
				Console.WriteLine("L2-ALT-MISS " + row + " peer=" + peer.Name + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) +
					" missWarnings=" + misses.Count);
				Assert.IsTrue(misses.Count >= MISS_RETRIES,
					"Missed peer " + peer.Name + " not retried every tend: " + TestL2Topology.Fmt(TestL2Topology.Warnings));

				string message = misses[0];
				Console.WriteLine("L2-ALT-MISS-LOG " + row + ' ' + message);

				foreach (string part in new string[] {peer.Name, endpoint, "alternate", translator, "pass_through"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
				CollectionAssert.AreEqual(new List<string> {seed.Name}, L2Topology.NodeNames(client));
				TestL2Topology.AssertSeedKeysWork(row, client, seed);
			}
		}

		private static void RequireAlternates()
		{
			TestL2Topology.SkipUnless(L2Topology.ReachableNode.Length > 0, "Alternate list cases" + BLOCKER);
		}

		private static ClientPolicy AlternatePolicy()
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.useServicesAlternate = true;
			return policy;
		}

		private static L2Node Reachable(List<L2Node> nodes)
		{
			foreach (L2Node node in nodes)
			{
				if (node.Proxy.Equals(L2Topology.ReachableNode))
				{
					Assert.IsTrue(!ReferenceEquals(node, nodes[0]), "Reachable node " + node + " must not be the first listener");
					return node;
				}
			}
			throw new InvalidOperationException("DISCOVERY_REACHABLE_NODE " + L2Topology.ReachableNode + " has no listener: " +
				TestL2Topology.Fmt(nodes));
		}

		private static List<L2Node> WithAlternate(List<L2Node> nodes)
		{
			List<L2Node> list = new List<L2Node>();

			foreach (L2Node node in nodes)
			{
				if (Alternates(node).Count > 0)
				{
					list.Add(node);
				}
			}
			Assert.IsTrue(list.Count >= 2, "Fewer than two nodes with an alternate address: " + TestL2Topology.Fmt(nodes));
			return list;
		}

		private static List<string> Alternates(L2Node node)
		{
			string reply = Info.Request(L2Topology.Host, node.Port, "service-clear-alt");
			List<string> list = new List<string>();

			if (!string.IsNullOrEmpty(reply))
			{
				foreach (Host h in Host.ParseServiceHosts(reply))
				{
					list.Add(h.name + ':' + h.port);
				}
			}
			return list;
		}

		private static List<string> Names(List<L2Node> nodes)
		{
			List<string> names = new List<string>();

			foreach (L2Node node in nodes)
			{
				names.Add(node.Name);
			}
			names.Sort(StringComparer.Ordinal);
			return names;
		}

		private static string HostOf(string endpoint)
		{
			return endpoint.Substring(0, endpoint.LastIndexOf(':'));
		}

		/// <summary>
		/// Assert the node's host is the advertised <c>host:port</c>, by name or by one of the
		/// addresses the name resolves to.
		/// </summary>
		private static void AssertAddress(string advertised, Host h)
		{
			string name = HostOf(advertised);
			int port = int.Parse(advertised.Substring(advertised.LastIndexOf(':') + 1));
			List<string> addresses = new List<string>();

			foreach (IPAddress address in Dns.GetHostAddresses(name))
			{
				addresses.Add(address.ToString());
			}
			Assert.IsTrue(h.name.Equals(name) || addresses.Contains(h.name), h.name + " is not " + name + TestL2Topology.Fmt(addresses));
			Assert.AreEqual(port, h.port);
		}

		private static List<string> AlternateMissing(L2Node node)
		{
			List<string> list = new List<string>();

			foreach (string error in TestL2Topology.Errors)
			{
				if (error.Contains("Node " + node.Name) && error.Contains("useServicesAlternate"))
				{
					list.Add(error);
				}
			}
			return list;
		}

		private static List<string> TranslationMisses()
		{
			List<string> list = new List<string>();

			foreach (string message in TestL2Topology.Warnings)
			{
				if (message.Contains("has no mapping"))
				{
					list.Add(message);
				}
			}

			foreach (string message in TestL2Topology.Errors)
			{
				if (message.Contains("has no mapping"))
				{
					list.Add(message);
				}
			}
			return list;
		}
	}
}
