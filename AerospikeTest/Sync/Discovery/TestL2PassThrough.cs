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
using System.Net.Sockets;
using Aerospike.Client;
using Joined = Aerospike.Test.L2Topology.Joined;
using L2Node = Aerospike.Test.L2Topology.L2Node;
using Peer = Aerospike.Test.L2Topology.Peer;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3 rows N-15, N-16 and N-18 against the pass-through profiles of
	/// test/discovery/run_l2.sh. In pass-through-names (Branch A) each node advertises its own
	/// alternate-access-address hostname with its own listener port; in pass-through-ports (Branch B)
	/// every node advertises one hostname with its own listener port. Every hostname resolves to the
	/// one Toxiproxy container in the client container, so every listener shares one IP address and the
	/// nodes differ only by port. DISCOVERY_PASS_THROUGH_BRANCH names the branch; without it every case
	/// is skipped.
	/// </summary>
	[TestClass]
	public class TestL2PassThrough
	{
		private const string SET = "l2-pass-through";
		private const int KEY_COUNT = 300;
		private const int COLLISION_RETRIES = 3;
		private const string BRANCH_A = " needs DISCOVERY_PASS_THROUGH_BRANCH=A, each node advertising its own " +
			"alternate-access-address hostname (run_l2.sh L2_PROFILE=pass-through-names)";
		private const string BRANCH_B = " needs DISCOVERY_PASS_THROUGH_BRANCH=B and DISCOVERY_CONTROL_DIR, every " +
			"node advertising one alternate-access-address hostname with its own port (run_l2.sh L2_PROFILE=pass-through-ports)";

		private readonly List<string> addedProxies = new List<string>();
		private readonly List<string> addedNodes = new List<string>();

		/// <summary>
		/// Translator that maps every endpoint to one host.
		/// </summary>
		public sealed class SingleAddressTranslator : IAddressTranslator
		{
			private readonly Host target;

			public SingleAddressTranslator(Host target)
			{
				this.target = target;
			}

			public Host Translate(Endpoint advertised)
			{
				return target;
			}
		}

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			foreach (string proxy in addedProxies)
			{
				L2Topology.RemoveListener(proxy);
			}
			addedProxies.Clear();

			foreach (string node in addedNodes)
			{
				L2Topology.Control("stop " + node);
			}

			if (addedNodes.Count > 0)
			{
				addedNodes.Clear();
				L2Topology.WaitClusterStable(L2Topology.Nodes().Count);
			}
		}

		/// <summary>
		/// N-15: pass-through, Branch A. Each node advertises its own alternate-access-address hostname
		/// and its own listener port, and the client reads the alternate list through
		/// <see cref="PassThroughAddressTranslator"/> with no map. Every partition owner is a distinct
		/// <see cref="Node"/> at the address its node advertises, and writes and reads are served by the owner
		/// without a server proxy hop. One Toxiproxy container serves every listener, so every hostname
		/// resolves to the same IP address and the nodes differ only by port.
		/// </summary>
		[TestMethod]
		public void N15DistinctNamesRouteToEveryOwner()
		{
			TestL2Topology.SkipUnless("A".Equals(L2Topology.PassThroughBranch), "N-15" + BRANCH_A);

			List<L2Node> nodes = L2Topology.Nodes();
			HashSet<string> hostnames = new HashSet<string>();

			foreach (L2Node node in nodes)
			{
				string alternate = Alternate(node);
				Console.WriteLine("L2-N15-ADVERTISED " + node.Name + " standard=" + TestL2Topology.Fmt(node.Advertised) + " alternate=" + alternate);
				hostnames.Add(HostOf(alternate));
				Assert.AreEqual(node.Port, PortOf(alternate));
			}
			Assert.AreEqual(nodes.Count, hostnames.Count, "Hostnames " + TestL2Topology.Fmt(hostnames));
			AssertRoutesToOwners("N-15", PassThroughPolicy(), nodes);
		}

		/// <summary>
		/// N-16: pass-through, Branch B. Every node advertises the same alternate-access-address
		/// hostname and its own alternate-access-port, the client reads the alternate list through
		/// <see cref="PassThroughAddressTranslator"/> with no map, and the partition map is complete with
		/// every owner a distinct <see cref="Node"/> served without a server proxy hop. The server resolves the
		/// hostname at startup, so run_l2.sh also makes it resolve in the server containers.
		/// </summary>
		[TestMethod]
		public void N16SharedNameRoutesToEveryOwner()
		{
			RequireBranchB("N-16");

			List<L2Node> nodes = L2Topology.Nodes();
			HashSet<string> hostnames = new HashSet<string>();

			foreach (L2Node node in nodes)
			{
				string alternate = Alternate(node);
				Console.WriteLine("L2-N16-ADVERTISED " + node.Name + " standard=" + TestL2Topology.Fmt(node.Advertised) + " alternate=" + alternate);
				hostnames.Add(HostOf(alternate));
				Assert.AreEqual(node.Port, PortOf(alternate));
			}
			Assert.AreEqual(1, hostnames.Count, "Hostnames " + TestL2Topology.Fmt(hostnames));
			AssertRoutesToOwners("N-16", PassThroughPolicy(), nodes);
		}

		/// <summary>
		/// N-16 with on_miss strict: <see cref="PassThroughAddressTranslator"/> never misses, so routing is
		/// the same as with the default.
		/// </summary>
		[TestMethod]
		public void N16SharedNameStrictRoutesToEveryOwner()
		{
			RequireBranchB("N-16 strict");

			ClientPolicy policy = PassThroughPolicy();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertRoutesToOwners("N-16-strict", policy, L2Topology.Nodes());
		}

		/// <summary>
		/// N-16 scale-out: a node started mid-test that advertises the same hostname and its own port is
		/// an active <see cref="Node"/> within 2 x tend_interval of the peers-generation bump that announces
		/// it, with no client change. The test gives it a listener on that port before it joins.
		/// </summary>
		[TestMethod]
		public void N16NewNodeOnSharedNameActiveWithinTwoTendIntervals()
		{
			RequireBranchB("N-16 scale-out");

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			List<string> names = L2Topology.NodeNames();
			string hostname = HostOf(Alternate(first));
			ClientPolicy policy = PassThroughPolicy();

			using (AerospikeClient client = new AerospikeClient(policy, SeedOf(first)))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.AssertAllPartitionsOwned("N-16-before", client);

				long generation = L2Topology.PeersGeneration(first);
				int port = L2Topology.NextListenerPort();
				string container = Start("alternate-port=" + port);
				L2Topology.AddListener(container, port, container + ':' + L2Topology.UpstreamPort(first.Proxy));
				addedProxies.Add(container);
				Assert.AreEqual(generation, L2Topology.PeersGeneration(first),
					"peers-generation bumped before " + container + " had a listener");

				Joined joined = L2Topology.WaitJoined(client, first, generation, names);
				Node node = joined.Node;
				long budget = 2L * policy.tendInterval;
				Dictionary<string, string> info = Info.Request(L2Topology.Host, port, "node", "service-clear-alt");
				Console.WriteLine("L2-N16-TIMING node=" + node.Name + " host=" + node.Host + " container=" + container +
					" advertised=" + TestL2Topology.Get(info, "service-clear-alt") + " generation=" + generation + "->" + joined.Generation +
					" bumpToActiveMs=" + joined.BumpToActiveMs + " tendInterval=" + policy.tendInterval + " budgetMs=" + budget);
				Assert.AreEqual(TestL2Topology.Get(info, "node"), node.Name);
				Assert.IsTrue(TestL2Topology.Get(info, "service-clear-alt").Contains(hostname + ':' + port),
					hostname + ':' + port + " not in " + TestL2Topology.FmtMap(info));
				Assert.AreEqual(port, node.Host.port);
				Assert.IsTrue(joined.BumpToActiveMs <= budget,
					node.Name + " active " + joined.BumpToActiveMs + "ms after the bump, budget " + budget + "ms");

				bool owns = L2Topology.WaitFor(() => L2Topology.Owners(client).ContainsKey(node.Name));
				Console.WriteLine("L2-N16-OWNERS " + TestL2Topology.FmtMap(L2Topology.Owners(client)));
				Assert.IsTrue(owns, node.Name + " owns no partitions");
			}
		}

		/// <summary>
		/// N-18 on pass-through: two nodes advertise the same alternate-access-address and
		/// alternate-access-port. Seeded through a node that does not collide, neither colliding node
		/// becomes a <see cref="Node"/>, the <see cref="AerospikeException.TranslationCollision"/> message names both
		/// node ids, the <c>host:port</c> and the translator on every tend, and the other nodes keep
		/// serving their partitions. The shared port gets a listener to one of the two only once both
		/// are in the cluster, so neither was ever a <see cref="Node"/>.
		/// </summary>
		[TestMethod]
		public void N18PassThroughCollidingNodesCreateNoNode()
		{
			RequireBranchB("N-18 pass-through");
			AssertCollidingNodesCreateNoNode("N-18-pass-through", PassThroughPolicy(), "pass-through");
		}

		/// <summary>
		/// N-18 with the default translator: as on pass-through, named static-map.
		/// </summary>
		[TestMethod]
		public void N18StaticMapCollidingNodesCreateNoNode()
		{
			RequireBranchB("N-18 static-map");

			ClientPolicy policy = L2Topology.Policy();
			policy.useServicesAlternate = true;
			AssertCollidingNodesCreateNoNode("N-18-static-map", policy, "static-map");
		}

		/// <summary>
		/// N-18 with a custom translator that maps every endpoint to the seed's address: no peer becomes
		/// a <see cref="Node"/>, the <see cref="AerospikeException.TranslationCollision"/> message names the peer,
		/// the seed, the address and the translator class on every tend, and the seed keeps serving its
		/// partitions.
		/// </summary>
		[TestMethod]
		public void N18CustomTranslatorCollidingNodesCreateNoNode()
		{
			RequireBranchB("N-18 custom");

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			Host seed = SeedOf(first);
			Host target = new Host(FirstAddress(seed.name), seed.port);
			ClientPolicy policy = L2Topology.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = new SingleAddressTranslator(target);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, seed))
			{
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(new List<string> {first.Name}, L2Topology.NodeNames(client));

					foreach (L2Node peer in nodes.GetRange(1, nodes.Count - 1))
					{
						if (Collisions(peer.Name).Count < COLLISION_RETRIES)
						{
							return false;
						}
					}
					return true;
				});

				foreach (L2Node peer in nodes.GetRange(1, nodes.Count - 1))
				{
					List<string> collisions = Collisions(peer.Name);
					Console.WriteLine("L2-N18-CUSTOM-RESULT peer=" + peer.Name + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) +
						" target=" + target + " collisionWarnings=" + collisions.Count);
					Assert.IsTrue(collisions.Count >= COLLISION_RETRIES,
						"Colliding peer " + peer.Name + " not retried every tend: " + TestL2Topology.Fmt(TestL2Topology.Warnings));
					Console.WriteLine("L2-N18-CUSTOM-LOG " + collisions[0]);
					AssertCollision(collisions[0], peer.Name, first.Name, target, typeof(SingleAddressTranslator).FullName);
				}
				CollectionAssert.AreEqual(new List<string> {first.Name}, L2Topology.NodeNames(client));
				TestL2Topology.AssertSeedKeysWork("N-18-custom", client, first);
			}
		}

		/// <summary>
		/// N-18 on port-map: two nodes are mapped to the same <c>host:port</c>. Seeded through the node
		/// that does not collide, neither colliding node becomes a <see cref="Node"/>, the
		/// <see cref="AerospikeException.TranslationCollision"/> message names both node ids, the
		/// <c>host:port</c> and the translator on every tend, and the seed keeps serving its partitions.
		/// </summary>
		[TestMethod]
		public void N18PortMapCollidingNodesCreateNoNode()
		{
			RequireBranchB("N-18 port-map");

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			L2Node a = nodes[1];
			L2Node b = nodes[2];
			Dictionary<string, string> portMap = new Dictionary<string, string>(L2Topology.PortMapByEndpoint(nodes));

			foreach (string advertised in b.Advertised)
			{
				portMap[advertised] = L2Topology.Listener(a.Port);
			}
			Console.WriteLine("L2-N18-PORT-MAP " + TestL2Topology.FmtMap(portMap));

			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(portMap);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(new List<string> {first.Name}, L2Topology.NodeNames(client));
					return Collisions(a.Name).Count >= COLLISION_RETRIES && Collisions(b.Name).Count >= COLLISION_RETRIES;
				});

				string message = Collisions(a.Name)[0];
				Console.WriteLine("L2-N18-PORT-MAP-RESULT nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " colliding=[" + a.Name +
					", " + b.Name + "] collisionWarnings=" + Collisions(a.Name).Count + '/' + Collisions(b.Name).Count);
				Console.WriteLine("L2-N18-PORT-MAP-LOG " + message);
				CollectionAssert.AreEqual(new List<string> {first.Name}, L2Topology.NodeNames(client));
				AssertCollision(message, a.Name, b.Name, new Host(L2Topology.Host, a.Port), "port-map");
				TestL2Topology.AssertSeedKeysWork("N-18-port-map", client, first);
			}
		}

		/// <summary>
		/// N-18 newcomer: a node started mid-test advertises the alternate-access-address and port of a
		/// node that is already a <see cref="Node"/>. No <see cref="Node"/> is created for the newcomer's endpoint,
		/// under the newcomer's name or under the name of the node that answers on it, and the collision
		/// is logged on every tend. FR-46 does not say what happens to the existing <see cref="Node"/>; the
		/// test reports it and does not assert it. The wait stops at the first extra <see cref="Node"/>, since
		/// each holds connections to the existing node.
		/// </summary>
		[TestMethod]
		public void N18PassThroughNewcomerCollidingWithNodeIsNotCreated()
		{
			RequireBranchB("N-18 newcomer");

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			L2Node existing = nodes[nodes.Count - 1];
			List<string> names = L2Topology.NodeNames();
			string endpoint = Alternate(existing);

			using (AerospikeClient client = new AerospikeClient(PassThroughPolicy(), SeedOf(first)))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.AssertAllPartitionsOwned("N-18-newcomer-before", client);
				Node before = client.Cluster.GetNode(existing.Name);
				TestL2Topology.CaptureWarnings();

				Start("alternate-port=" + PortOf(endpoint));
				L2Topology.WaitClusterStable(nodes.Count + 1);

				List<string> advertising = PeerNames(first, endpoint);
				advertising.Remove(existing.Name);
				Console.WriteLine("L2-N18-NEWCOMER-ADVERTISED endpoint=" + endpoint + " existing=" + existing.Name +
					" newcomer=" + TestL2Topology.Fmt(advertising) + " peers-clear-alt=" + Info.Request(L2Topology.Host, first.Port, "peers-clear-alt"));
				Assert.AreEqual(1, advertising.Count, "Newcomers advertising " + endpoint);

				string newcomer = advertising[0];
				L2Topology.WaitFor(() => Collisions(newcomer).Count >= COLLISION_RETRIES ||
					client.Nodes.Length > names.Count);

				List<string> collisions = Collisions(newcomer);
				List<string> nodeNames = L2Topology.NodeNames(client);
				Node after = client.Cluster.GetNode(existing.Name);
				int instances = nodeNames.Count(n => n == existing.Name);

				Console.WriteLine("L2-N18-NEWCOMER-RESULT newcomer=" + newcomer + " nodes=" + TestL2Topology.Fmt(nodeNames) +
					" collisionWarnings=" + collisions.Count);
				Console.WriteLine("L2-N18-EXISTING node=" + existing.Name + " kept=" + nodeNames.Contains(existing.Name) +
					" sameInstance=" + ReferenceEquals(before, after) + " instances=" + instances + " active=" + (after != null && after.Active) +
					" host=" + (after == null ? null : after.Host) + " owners=" + TestL2Topology.FmtMap(L2Topology.Owners(client)));
				Assert.IsFalse(nodeNames.Contains(newcomer), newcomer + " is a Node: " + TestL2Topology.Fmt(nodeNames));
				CollectionAssert.AreEqual(names, nodeNames,
					"Node created for " + newcomer + "'s endpoint " + endpoint + ", answered by " + existing.Name);
				Assert.IsTrue(collisions.Count >= COLLISION_RETRIES,
					"No collision logged for " + newcomer + ": " + TestL2Topology.Fmt(TestL2Topology.Warnings));

				string message = collisions[0];
				Console.WriteLine("L2-N18-NEWCOMER-LOG " + message);
				AssertCollision(message, newcomer, existing.Name, new Host(HostOf(endpoint), PortOf(endpoint)), "pass-through");
			}
		}

		private void AssertCollidingNodesCreateNoNode(string row, ClientPolicy policy, string translator)
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			List<string> names = L2Topology.NodeNames();
			string hostname = HostOf(Alternate(first));
			int port = L2Topology.NextListenerPort();
			string endpoint = hostname + ':' + port;
			string container = Start("alternate-port=" + port);
			Start("alternate-port=" + port);
			L2Topology.WaitClusterStable(nodes.Count + 2);
			L2Topology.AddListener(container, port, container + ':' + L2Topology.UpstreamPort(first.Proxy));
			addedProxies.Add(container);

			List<string> colliding = PeerNames(first, endpoint);
			Console.WriteLine("L2-" + row + "-ADVERTISED endpoint=" + endpoint + " nodes=" + TestL2Topology.Fmt(colliding) +
				" peers-clear-alt=" + Info.Request(L2Topology.Host, first.Port, "peers-clear-alt"));
			Assert.AreEqual(2, colliding.Count, "Nodes advertising " + endpoint);

			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, SeedOf(first)))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
					return Collisions(colliding[0]).Count >= COLLISION_RETRIES &&
						Collisions(colliding[1]).Count >= COLLISION_RETRIES;
				});

				string message = Collisions(colliding[0])[0];
				Console.WriteLine("L2-" + row + "-RESULT nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " colliding=" + TestL2Topology.Fmt(colliding) +
					" collisionWarnings=" + Collisions(colliding[0]).Count + '/' + Collisions(colliding[1]).Count);
				Console.WriteLine("L2-" + row + "-LOG " + message);
				CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
				Assert.IsTrue(Collisions(colliding[1]).Count >= COLLISION_RETRIES,
					"Colliding node " + colliding[1] + " not retried every tend: " + TestL2Topology.Fmt(TestL2Topology.Warnings));
				AssertCollision(message, colliding[0], colliding[1], new Host(hostname, port), translator);
				AssertOthersServe(row, client, names);
			}
		}

		private void RequireBranchB(string row)
		{
			TestL2Topology.SkipUnless("B".Equals(L2Topology.PassThroughBranch) && L2Topology.ControlDir.Length > 0, row + BRANCH_B);
		}

		private string Start(string options)
		{
			string container = L2Topology.Control("start " + options)["container"];
			Assert.IsNotNull(container);
			addedNodes.Add(container);
			return container;
		}

		private static void AssertRoutesToOwners(string row, ClientPolicy policy, List<L2Node> nodes)
		{
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, SeedOf(nodes[0])))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned(row, client);
				HashSet<Node> distinct = new HashSet<Node>(ReferenceEqualityComparer.Instance);

				foreach (Node master in masters)
				{
					distinct.Add(master);
				}
				Dictionary<string, int> owners = L2Topology.Owners(masters);
				Console.WriteLine("L2-" + row + "-OWNERS nodes=" + distinct.Count + " partitionsPerNode=" + TestL2Topology.FmtMap(owners));
				Assert.AreEqual(nodes.Count, distinct.Count);
				CollectionAssert.AreEqual(L2Topology.NodeNames(), TestL2Topology.Sorted(owners.Keys));

				foreach (Node node in client.Nodes)
				{
					Host h = node.Host;
					string alternate = Alternate(NodeOf(nodes, node.Name));
					Console.WriteLine("L2-" + row + "-NODE " + node.Name + " host=" + h.name + ':' + h.port +
						" alternate=" + alternate);
					Assert.AreSame(node, client.Cluster.GetNode(node.Name), node.Name);
					Assert.IsTrue(distinct.Contains(node), node.Name);
					AssertAddress(alternate, h);
				}
				AssertServedByOwners(row, client, masters, nodes);
				Assert.AreEqual(0, TestL2Topology.Warnings.Count, TestL2Topology.Fmt(TestL2Topology.Warnings));
			}
		}

		/// <summary>
		/// Write and read keys spread across partitions with replica MASTER and assert, from each
		/// node's namespace statistics, that each node served exactly the keys it owns with no server
		/// proxy hop.
		/// </summary>
		private static void AssertServedByOwners(string row, AerospikeClient client, Node[] masters, List<L2Node> nodes)
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
				Key key = new Key(L2Topology.Namespace, SET, row + '-' + i);
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
				Console.WriteLine("L2-" + row + "-SERVED node=" + node.Name + " keys=" + expected + " writes=" + writes +
					" reads=" + reads + " proxied=" + proxied);
				Assert.IsTrue(expected > 0, node.Name + " served no keys");
				Assert.AreEqual(expected, writes, node.Name + " writes");
				Assert.AreEqual(expected, reads, node.Name + " reads");
				Assert.AreEqual(0L, proxied, node.Name + " proxied");
			}
		}

		/// <summary>
		/// Assert that every partition with a master in the client's map is owned by one of
		/// <c>names</c> and serves writes and reads, and that the partitions of the colliding nodes
		/// have no master.
		/// </summary>
		private static void AssertOthersServe(string row, AerospikeClient client, List<string> names)
		{
			Node[] masters = client.Cluster.partitionMap[L2Topology.Namespace].replicas[0];
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			wp.replica = Replica.MASTER;
			Policy rp = new Policy(client.ReadPolicyDefault);
			rp.replica = Replica.MASTER;
			int served = 0;
			int noMaster = 0;

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(L2Topology.Namespace, SET, row + '-' + i);
				Node node = masters[(int)Partition.GetPartitionId(key.digest)];

				if (node == null)
				{
					noMaster++;
					continue;
				}
				Assert.IsTrue(names.Contains(node.Name), node.Name);
				client.Put(wp, key, new Bin("v", i));
				Assert.AreEqual(i, client.Get(rp, key).GetInt("v"));
				served++;
			}
			Console.WriteLine("L2-" + row + "-KEYS served=" + served + " keysWithoutMaster=" + noMaster);
			Assert.IsTrue(served > 0);
			Assert.IsTrue(noMaster > 0);
		}

		private static void AssertCollision(string message, string nodeA, string nodeB, Host host, string translator)
		{
			List<string> expected = new List<string>
			{
				new AerospikeException.TranslationCollision(nodeA, nodeB, host, translator).BaseMessage,
				new AerospikeException.TranslationCollision(nodeB, nodeA, host, translator).BaseMessage
			};
			Assert.IsTrue(expected.Any(e => message.Contains(e)), message + " not in " + TestL2Topology.Fmt(expected));

			foreach (string part in new string[] {nodeA, nodeB, host.name + ':' + host.port, translator})
			{
				Assert.IsTrue(message.Contains(part), part + " not in: " + message);
			}
		}

		private static List<string> Collisions(string nodeName)
		{
			List<string> list = new List<string>();

			foreach (string warning in TestL2Topology.Warnings)
			{
				if (warning.Contains("to the same address") && warning.Contains(nodeName))
				{
					list.Add(warning);
				}
			}
			return list;
		}

		private static List<string> PeerNames(L2Node observer, string endpoint)
		{
			List<string> names = new List<string>();

			foreach (Peer peer in L2Topology.ParsePeers(Info.Request(L2Topology.Host, observer.Port, "peers-clear-alt")))
			{
				if (peer.Endpoints.Contains(endpoint))
				{
					names.Add(peer.Name);
				}
			}
			names.Sort(StringComparer.Ordinal);
			return names;
		}

		private static ClientPolicy PassThroughPolicy()
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			return policy;
		}

		private static Host SeedOf(L2Node node)
		{
			string alternate = Alternate(node);
			return new Host(HostOf(alternate), PortOf(alternate));
		}

		private static string Alternate(L2Node node)
		{
			List<string> list = L2Topology.Alternates(node);
			Assert.AreEqual(1, list.Count, node.Name + " alternate list " + TestL2Topology.Fmt(list));
			return list[0];
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

		private static string HostOf(string endpoint)
		{
			return endpoint.Substring(0, endpoint.LastIndexOf(':'));
		}

		private static int PortOf(string endpoint)
		{
			return int.Parse(endpoint.Substring(endpoint.LastIndexOf(':') + 1));
		}

		private static string FirstAddress(string name)
		{
			IPAddress[] addresses = Dns.GetHostAddresses(name);

			foreach (IPAddress address in addresses)
			{
				if (address.AddressFamily == AddressFamily.InterNetwork)
				{
					return address.ToString();
				}
			}
			return addresses[0].ToString();
		}

		/// <summary>
		/// Assert the node's host is the advertised <c>host:port</c>, by name or by one of the
		/// addresses the name resolves to.
		/// </summary>
		private static void AssertAddress(string advertised, Host h)
		{
			string name = HostOf(advertised);
			List<string> addresses = new List<string>();

			foreach (IPAddress address in Dns.GetHostAddresses(name))
			{
				addresses.Add(address.ToString());
			}
			Assert.IsTrue(h.name.Equals(name) || addresses.Contains(h.name), h.name + " is not " + name + TestL2Topology.Fmt(addresses));
			Assert.AreEqual(PortOf(advertised), h.port);
		}
	}
}
