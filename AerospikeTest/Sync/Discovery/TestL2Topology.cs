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
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;
using Peer = Aerospike.Test.L2Topology.Peer;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3 rows N-1 to N-14 against the <see cref="L2Topology"/>. N-3 is retired,
	/// N-10 is in <see cref="TestL2ReachableNode"/> and N-13 is not implemented. Every case seeds the first
	/// listener and builds its own <see cref="ClientPolicy"/>.
	/// </summary>
	[TestClass]
	public class TestL2Topology
	{
		private const string SET = "l2";
		private const int KEY_COUNT = 300;
		private const int MISS_RETRIES = 3;
		private const string DNS_BLOCKER = "N-4 needs a DNS server on the L2 client network serving SRV " +
			"records for the endpoint; the L2 topology has none yet (E4 gap)";
		private const string NODE_CONTROL_BLOCKER = "N-11 needs DISCOVERY_CONTROL_DIR so the host can start a " +
			"fourth node (run_l2.sh L2_NODE_CONTROL=1, or the discovery-l2 workflow)";
		private const int REFRESH_INTERVAL = 600000;
		private const int MISS_KEY_COUNT = 200;

		// Mirrors the Java blocker: the L2 topology has no SRV DNS server yet.
		private static readonly bool DnsServerAvailable = false;

		internal static readonly ConcurrentQueue<string> Warnings = new ConcurrentQueue<string>();
		internal static readonly ConcurrentQueue<string> Errors = new ConcurrentQueue<string>();

		private string disabledProxy;
		private readonly List<string> disabledProxies = new List<string>();
		private string addedProxy;
		private string addedNode;

		/// <summary>
		/// Translator that maps nothing, so every endpoint is a miss.
		/// </summary>
		public sealed class EmptyTranslator : IAddressTranslator
		{
			public Host Translate(Endpoint advertised)
			{
				return null;
			}
		}

		/// <summary>
		/// Translator that delegates to a function, so a test can swap the mapping mid run.
		/// </summary>
		internal sealed class FuncTranslator : IAddressTranslator
		{
			private readonly Func<Endpoint, Host> func;

			internal FuncTranslator(Func<Endpoint, Host> func)
			{
				this.func = func;
			}

			public Host Translate(Endpoint advertised)
			{
				return func(advertised);
			}
		}

		/// <summary>
		/// Seed provider that returns fixed seeds and records the time of every call.
		/// </summary>
		internal sealed class CountingSeedProvider : ISeedCandidateProvider
		{
			internal readonly ConcurrentQueue<long> calls = new ConcurrentQueue<long>();
			private readonly List<Host> seeds;

			internal CountingSeedProvider(params Host[] seeds)
			{
				this.seeds = new List<Host>(seeds);
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				calls.Enqueue(NanoTime());
				return seeds;
			}

			public bool NeedsPeriodicRefresh()
			{
				// False so SeedCandidateManager.RequestUrgentRefresh is a no-op: N-12 asserts the
				// client reseeds from the last snapshot without another provider call (Java parity).
				return false;
			}

			public bool SupportsDeadline()
			{
				return true;
			}

			internal long CallsBetween(long begin, long end)
			{
				return calls.Count(t => t >= begin && t <= end);
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

			if (disabledProxy != null)
			{
				L2Topology.SetListener(disabledProxy, true);
				disabledProxy = null;
			}

			foreach (string proxy in disabledProxies)
			{
				L2Topology.SetListener(proxy, true);
			}
			disabledProxies.Clear();

			if (addedProxy != null)
			{
				L2Topology.RemoveListener(addedProxy);
				addedProxy = null;
			}

			if (addedNode != null)
			{
				L2Topology.Control("stop " + addedNode);
				addedNode = null;
				L2Topology.WaitClusterStable(L2Topology.Nodes().Count);
			}
		}

		/// <summary>
		/// N-1: baseline, no translator. The client reaches the seed through the endpoint and then
		/// fails on the peers, whose advertised addresses it has no route to.
		/// </summary>
		[TestMethod]
		public void N1BaselineWithoutTranslatorFailsOnPeers()
		{
			Host seed = new Host(L2Topology.Host, L2Topology.Ports[0]);
			Dictionary<string, string> info = null;

			try
			{
				info = Info.Request(seed.name, seed.port, "node", "peers-clear-std");
			}
			catch (AerospikeException ae)
			{
				Assert.Fail("N-1 guard: seed " + seed + " is not reachable through the endpoint: " + ae.Message);
			}

			string node = Get(info, "node");
			string reply = Get(info, "peers-clear-std");
			List<Peer> peers = L2Topology.ParsePeers(reply);
			Console.WriteLine("L2-N1-GUARD seed=" + seed + " node=" + node + " peers-clear-std=" + reply);
			Assert.IsTrue(!string.IsNullOrEmpty(node), "N-1 guard: seed " + seed + " returned no node name");
			Assert.IsFalse(peers.Count == 0, "N-1 guard: seed " + node + " lists no peers: " + reply);

			foreach (Peer peer in peers)
			{
				foreach (string endpoint in peer.Endpoints)
				{
					L2Topology.AssertUnreachable(endpoint);
				}
			}

			ClientPolicy policy = L2Topology.Policy();

			try
			{
				using (AerospikeClient client = new AerospikeClient(policy, seed))
				{
					Assert.Fail("N-1: client joined " + Fmt(L2Topology.NodeNames(client)) + " without a translator");
				}
			}
			catch (AerospikeException ae)
			{
				string message = ae.Message;
				Console.WriteLine("L2-N1-ERROR " + message);
				Assert.IsTrue(message.Contains("Peers not reachable: "), message);

				foreach (Peer peer in peers)
				{
					foreach (string endpoint in peer.Endpoints)
					{
						int index = endpoint.LastIndexOf(':');
						string unreachable = endpoint.Substring(0, index) + ' ' + endpoint.Substring(index + 1);
						Assert.IsTrue(message.Contains(unreachable), unreachable + " not named in: " + message);
					}
				}
			}
		}

		/// <summary>
		/// N-2: port-map keyed by advertised <c>(host, port)</c>. All nodes join and the partition
		/// map is complete.
		/// </summary>
		[TestMethod]
		public void N2PortMapByHostPortJoinsAllNodes()
		{
			AssertPortMapJoins("N-2-host-port", L2Topology.PortMapByEndpoint(L2Topology.Nodes()));
		}

		/// <summary>
		/// N-2: port-map keyed by node name. All nodes join and the partition map is complete.
		/// </summary>
		[TestMethod]
		public void N2PortMapByNodeNameJoinsAllNodes()
		{
			AssertPortMapJoins("N-2-node-name", L2Topology.PortMapByNodeName(L2Topology.Nodes()));
		}

		/// <summary>
		/// N-4: dns provider end to end. The seed comes from SRV records for the endpoint, one per
		/// listener, and the peers through port-map.
		/// </summary>
		[TestMethod]
		public void N4DnsProviderSeedsThroughEndpoint()
		{
			SkipUnless(DnsServerAvailable, DNS_BLOCKER);

			ClientPolicy policy = L2Topology.Policy();
			policy.seedCandidateProvider = DnsSeedCandidateProvider.CreateBuilder()
				.Names("_aerospike._tcp." + L2Topology.Host)
				.RecordType(DnsSeedCandidateProvider.RecordType.SRV)
				.Build();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(L2Topology.Nodes()));

			// Pass Host[] so we hit the public seed ctor, not the InternalsVisibleTo
			// AerospikeClient(ClientPolicy) helper that leaves cluster null.
			using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("N-4", client);
			}
		}

		/// <summary>
		/// N-5: no SeedOnlyCluster. Every node owns partitions as its own <see cref="Node"/>, and writes and
		/// reads for keys spread across partitions are served by the owner without a server proxy hop.
		/// </summary>
		[TestMethod]
		public void N5EveryPartitionOwnerIsADistinctNode()
		{
			List<L2Node> nodes = L2Topology.Nodes();
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned("N-5", client);
				HashSet<Node> distinct = new HashSet<Node>(ReferenceEqualityComparer.Instance);

				foreach (Node master in masters)
				{
					distinct.Add(master);
				}
				Dictionary<string, int> owners = L2Topology.Owners(masters);
				Console.WriteLine("L2-N5-OWNERS nodes=" + distinct.Count + " partitionsPerNode=" + FmtMap(owners));
				Assert.AreEqual(nodes.Count, distinct.Count);
				CollectionAssert.AreEqual(L2Topology.NodeNames(), Sorted(owners.Keys));

				foreach (Node node in client.Nodes)
				{
					Assert.AreSame(node, client.Cluster.GetNode(node.Name), node.Name);
					Assert.IsTrue(distinct.Contains(node), node.Name);
				}

				string[] stats = {"client_write_success", "client_read_success", "client_proxy_complete",
					"client_proxy_error", "client_proxy_timeout"};
				SortedDictionary<string, Dictionary<string, long>> before = new SortedDictionary<string, Dictionary<string, long>>(StringComparer.Ordinal);

				foreach (L2Node node in nodes)
				{
					before[node.Name] = L2Topology.NamespaceStats(node, stats);
				}

				Cluster cluster = client.Cluster;
				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				wp.replica = Replica.MASTER;
				Policy rp = new Policy(client.ReadPolicyDefault);
				rp.replica = Replica.MASTER;
				SortedDictionary<string, long> served = new SortedDictionary<string, long>(StringComparer.Ordinal);
				HashSet<int> partitions = new HashSet<int>();

				for (int i = 0; i < KEY_COUNT; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "n5-" + i);
					int partitionId = (int)Partition.GetPartitionId(key.digest);
					Node owner = masters[partitionId];
					Assert.AreSame(owner, Partition.Write(cluster, wp, key).GetNodeWrite(cluster));
					Assert.AreSame(owner, Partition.Read(cluster, rp, key).GetNodeRead(cluster));
					client.Put(wp, key, new Bin("v", i));
					Record record = client.Get(rp, key);
					Assert.AreEqual(i, record.GetInt("v"));
					served[owner.Name] = served.GetValueOrDefault(owner.Name, 0L) + 1;
					partitions.Add(partitionId);
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
					Console.WriteLine("L2-N5-SERVED node=" + node.Name + " partitions=" + owners[node.Name] +
						" keys=" + expected + " writes=" + writes + " reads=" + reads + " proxied=" + proxied);
					Assert.IsTrue(expected > 0, node.Name + " served no keys");
					Assert.AreEqual(expected, writes, node.Name + " writes");
					Assert.AreEqual(expected, reads, node.Name + " reads");
					Assert.AreEqual(0L, proxied, node.Name + " proxied");
				}
				Console.WriteLine("L2-N5-KEYS keys=" + KEY_COUNT + " partitions=" + partitions.Count);
			}
		}

		/// <summary>
		/// N-6: TLS through the proxy, ee only. The handshake validates the certificate against the
		/// node's tls-name, not the endpoint hostname the client connects to. The server certificate
		/// carries the tls-name only as a dNSName SAN, so no CN fallback warning is logged.
		/// </summary>
		[TestMethod]
		public void N6TlsValidatesAgainstNodeTlsName()
		{
			SkipUnless("ee".Equals(L2Topology.Subset),
				"N-6 needs DISCOVERY_SUBSET=ee for server TLS and TLS listeners; subset is " + L2Topology.Subset);

			List<L2Node> nodes = L2Topology.Nodes();

			foreach (L2Node node in nodes)
			{
				Assert.IsTrue(node.TlsPort > 0, "No TLS listener for " + node);
				Assert.IsFalse(node.TlsAdvertised.Count == 0, "No advertised TLS address for " + node);
				Assert.IsNotNull(node.TlsName, "No tls-name for " + node);
			}

			L2Node first = nodes[0];
			Assert.IsFalse(first.TlsName.Equals(L2Topology.Host), "tls-name " + first.TlsName + " is the endpoint hostname");

			ClientPolicy policy = L2Topology.Policy();
			policy.tlsPolicy = new TlsPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.TlsPortMap(nodes));
			CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, first.TlsName, first.TlsPort)))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("N-6", client);

				List<string> endpointAddresses = new List<string>();

				foreach (IPAddress address in Dns.GetHostAddresses(L2Topology.Host))
				{
					endpointAddresses.Add(address.ToString());
				}

				foreach (Node node in client.Nodes)
				{
					Host h = node.Host;
					L2Node expected = NodeOf(nodes, node.Name);
					Console.WriteLine("L2-N6-NODE " + node.Name + " host=" + h.name + ':' + h.port + " tlsName=" + h.tlsName +
						" endpoint=" + L2Topology.Host + Fmt(endpointAddresses));
					Assert.IsTrue(h.name.Equals(L2Topology.Host) || endpointAddresses.Contains(h.name),
						h.name + " is not " + L2Topology.Host + Fmt(endpointAddresses));
					Assert.AreEqual(expected.TlsPort, h.port);
					Assert.AreEqual(expected.TlsName, h.tlsName);
				}

				for (int i = 0; i < 20; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "n6-" + i);
					client.Put(null, key, new Bin("v", i));
					Assert.AreEqual(i, client.Get(null, key).GetInt("v"));
				}
			}

			try
			{
				using (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.Host, L2Topology.Host, first.TlsPort)))
				{
					Assert.Fail("N-6: TLS seed validated against endpoint hostname " + L2Topology.Host);
				}
			}
			catch (AerospikeException ae)
			{
				// The Java client reports "Invalid TLS name: <name>". The .NET client surfaces the SslStream
				// certificate name mismatch, so the message is recorded but not matched against Java's text.
				string message = Messages(ae);
				Console.WriteLine("L2-N6-ERROR " + message);
			}

			Console.WriteLine("L2-N6-CN-FALLBACK certShape=" + L2Topology.TlsCertShape + " warnings=" + Fmt(CnFallbackWarnings()));
			Assert.AreEqual(0, CnFallbackWarnings().Count, Fmt(CnFallbackWarnings()));
		}

		/// <summary>
		/// N-7: node removed from the endpoint. With one node's listener disabled the client serves
		/// the other nodes' partitions; once it is enabled again the node returns and no node is stuck.
		/// </summary>
		[TestMethod]
		public void N7NodeRemovedFromEndpointDegradesAndReturns()
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node victim = nodes[nodes.Count - 1];
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned("N-7-before", client);
				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				wp.maxRetries = 0;
				wp.totalTimeout = 2000;
				List<Key> victimKeys = new List<Key>();
				List<Key> otherKeys = new List<Key>();

				for (int i = 0; i < KEY_COUNT; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "n7-" + i);
					Node owner = masters[(int)Partition.GetPartitionId(key.digest)];
					(owner.Name.Equals(victim.Name) ? victimKeys : otherKeys).Add(key);
				}
				Assert.IsFalse(victimKeys.Count == 0);
				Assert.IsFalse(otherKeys.Count == 0);

				disabledProxy = victim.Proxy;
				L2Topology.SetListener(victim.Proxy, false);

				Assert.IsTrue(L2Topology.WaitFor(() => !Writes(client, wp, victimKeys[0])),
					"Writes to " + victim.Name + " still succeed with its listener disabled");

				int victimFailed = 0;

				foreach (Key key in victimKeys)
				{
					if (!Writes(client, wp, key))
					{
						victimFailed++;
					}
				}

				foreach (Key key in otherKeys)
				{
					client.Put(wp, key, new Bin("v", 1));
					Assert.AreEqual(1, client.Get(null, key).GetInt("v"));
				}

				List<string> active = new List<string>();

				foreach (Node node in client.Nodes)
				{
					if (node.Active && !node.Name.Equals(victim.Name))
					{
						active.Add(node.Name);
					}
				}
				active.Sort(StringComparer.Ordinal);

				List<string> others = new List<string>(L2Topology.NodeNames());
				others.Remove(victim.Name);
				Console.WriteLine("L2-N7-DEGRADED victim=" + victim.Name + " nodes=" + Fmt(L2Topology.NodeNames(client)) +
					" activeOthers=" + Fmt(active) + " otherKeysServed=" + otherKeys.Count + " victimKeysFailed=" + victimFailed +
					'/' + victimKeys.Count);
				CollectionAssert.AreEqual(others, active);
				Assert.AreEqual(victimKeys.Count, victimFailed);

				L2Topology.SetListener(victim.Proxy, true);
				disabledProxy = null;

				Assert.IsTrue(L2Topology.WaitFor(() => Writes(client, wp, victimKeys[0])),
					"Writes to " + victim.Name + " did not recover");
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("N-7-after", client);

				foreach (Node node in client.Nodes)
				{
					Assert.IsTrue(node.Active, node.Name + " is not active");
				}

				foreach (Key key in victimKeys)
				{
					client.Put(wp, key, new Bin("v", 2));
					Assert.AreEqual(2, client.Get(null, key).GetInt("v"));
				}

				foreach (Key key in otherKeys)
				{
					client.Put(wp, key, new Bin("v", 2));
					Assert.AreEqual(2, client.Get(null, key).GetInt("v"));
				}
				Console.WriteLine("L2-N7-RECOVERED nodes=" + Fmt(L2Topology.NodeNames(client)) + " keys=" + KEY_COUNT);
			}
		}

		/// <summary>
		/// N-8: on_miss strict. A peer with no mapping produces the UX-2 error naming the node, the
		/// advertised endpoint and the translator, and creates no <see cref="Node"/>: its partitions have no
		/// master in the client's map. The other nodes keep serving every key and the missed peer is
		/// retried every tend.
		/// </summary>
		[TestMethod]
		public void N8StrictMissCreatesNoNode()
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node victim = nodes[nodes.Count - 1];
			List<L2Node> mapped = new List<L2Node>(nodes);
			mapped.Remove(victim);
			List<string> mappedNames = new List<string>();

			foreach (L2Node node in mapped)
			{
				mappedNames.Add(node.Name);
			}
			mappedNames.Sort(StringComparer.Ordinal);

			CaptureWarnings();

			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(mapped));
			policy.translationOnMiss = TranslationMissMode.STRICT;

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, mappedNames);
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(mappedNames, L2Topology.NodeNames(client));
					return Misses(victim).Count >= MISS_RETRIES;
				});

				List<string> misses = Misses(victim);
				Console.WriteLine("L2-N8-RESULT victim=" + victim.Name + " nodes=" + Fmt(L2Topology.NodeNames(client)) +
					" missWarnings=" + misses.Count);
				Assert.IsTrue(misses.Count >= MISS_RETRIES, "Missed peer " + victim.Name + " not retried every tend: " + Fmt(Warnings));
				CollectionAssert.AreEqual(mappedNames, L2Topology.NodeNames(client));

				string message = misses[0];
				Console.WriteLine("L2-N8-LOG " + message);

				List<string> expected = new List<string>();

				foreach (string advertised in victim.Advertised)
				{
					int index = advertised.LastIndexOf(':');
					Endpoint endpoint = new Endpoint(victim.Name, null, advertised.Substring(0, index),
						int.Parse(advertised.Substring(index + 1)), Endpoint.SourceList.STANDARD);
					expected.Add(new AerospikeException.TranslationMiss(endpoint, "port-map").BaseMessage);
				}
				Assert.IsTrue(expected.Any(e => message.Contains(e)), message + " not in " + Fmt(expected));

				foreach (string part in new string[] {victim.Name, victim.Advertised[0], "port-map", "pass_through"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
				AssertOwnedKeysWork(client, mappedNames, victim);
			}
		}

		/// <summary>
		/// N-11: a node started mid-test is an active <see cref="Node"/> in the client within 2 x
		/// tend_interval of the peers-generation bump that announces it, and eventually owns
		/// partitions. The host starts the node through DISCOVERY_CONTROL_DIR and the test gives it a
		/// listener and a port-map entry before it joins.
		/// </summary>
		[TestMethod]
		public void N11NewNodeActiveWithinTwoTendIntervals()
		{
			SkipUnless(L2Topology.ControlDir.Length > 0, NODE_CONTROL_BLOCKER);

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			List<string> names = L2Topology.NodeNames();
			PortMapAddressTranslator[] portMap = {new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes))};
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new FuncTranslator(advertised => Volatile.Read(ref portMap[0]).Translate(advertised));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.AssertAllPartitionsOwned("N-11-before", client);

				long generation = L2Topology.PeersGeneration(first);
				Dictionary<string, string> ack = L2Topology.Control("start");
				addedNode = ack["container"];
				int upstreamPort = L2Topology.UpstreamPort(first.Proxy);
				int port = L2Topology.AddListener(addedNode, addedNode + ':' + upstreamPort);
				addedProxy = addedNode;

				string advertised = ack["address"] + ':' + upstreamPort;
				Dictionary<string, string> map = new Dictionary<string, string>(L2Topology.PortMapByEndpoint(nodes));
				map[advertised] = L2Topology.Listener(port);
				Volatile.Write(ref portMap[0], new PortMapAddressTranslator(map));
				Console.WriteLine("L2-N11-MAPPED " + advertised + "=" + L2Topology.Listener(port));

				long mappedGeneration = L2Topology.PeersGeneration(first);
				Assert.AreEqual(generation, mappedGeneration,
					"peers-generation bumped before " + addedNode + " had a listener and a mapping");

				long[] bump = {0};
				long[] active = {0};
				Node[] added = {null};

				Assert.IsTrue(L2Topology.WaitFor(() =>
				{
					long now = NanoTime();

					if (bump[0] == 0 && L2Topology.PeersGeneration(first) != generation)
					{
						bump[0] = now;
					}

					if (active[0] == 0)
					{
						foreach (Node node in client.Nodes)
						{
							if (node.Active && !names.Contains(node.Name))
							{
								active[0] = now;
								added[0] = node;
							}
						}
					}
					return bump[0] != 0 && active[0] != 0;
				}, 5), "No peers-generation bump or no active node for " + addedNode);

				long elapsed = (active[0] - bump[0]) / 1000000L;
				long budget = 2L * policy.tendInterval;
				Node joined = added[0];
				Dictionary<string, string> info = Info.Request(L2Topology.Host, port, "node", "service-clear-std");
				Console.WriteLine("L2-N11-TIMING node=" + joined.Name + " host=" + joined.Host + " container=" + addedNode +
					" generation=" + generation + "->" + L2Topology.PeersGeneration(first) + " bumpToActiveMs=" + elapsed +
					" tendInterval=" + policy.tendInterval + " budgetMs=" + budget);
				Assert.AreEqual(Get(info, "node"), joined.Name);
				Assert.IsTrue(Get(info, "service-clear-std").Contains(advertised), advertised + " not in " + FmtMap(info));
				Assert.AreEqual(port, joined.Host.port);
				Assert.IsTrue(elapsed <= budget, joined.Name + " active " + elapsed + "ms after the bump, budget " + budget + "ms");

				bool owns = L2Topology.WaitFor(() => L2Topology.Owners(client).ContainsKey(joined.Name));
				Console.WriteLine("L2-N11-OWNERS " + FmtMap(L2Topology.Owners(client)));
				Assert.IsTrue(owns, joined.Name + " owns no partitions");
			}
		}

		/// <summary>
		/// N-12: re-seed on zero active nodes. With every listener disabled the client drops to zero
		/// nodes; once they are enabled again it re-seeds from the seeds it holds within 2 x
		/// tend_interval plus the connect timeout, without a seed provider refresh, and the partition
		/// map is complete again.
		/// </summary>
		[TestMethod]
		public void N12ReseedOnZeroActiveNodes()
		{
			List<L2Node> nodes = L2Topology.Nodes();
			CountingSeedProvider provider = new CountingSeedProvider(L2Topology.Seed());
			ClientPolicy policy = L2Topology.Policy();
			policy.seedCandidateProvider = provider;
			policy.discoveryRefreshIntervalMs = REFRESH_INTERVAL;
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));

			// Seed comes from the provider (same as Java new AerospikeClient(policy)).
			AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed());
			try
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("N-12-before", client);

				foreach (string proxy in L2Topology.ProxyPorts.Keys)
				{
					disabledProxies.Add(proxy);
					L2Topology.SetListener(proxy, false);
				}
				Assert.IsTrue(L2Topology.WaitFor(() => client.Nodes.Length == 0),
					"Client kept nodes " + Fmt(L2Topology.NodeNames(client)) + " with every listener disabled");

				int callsBefore = provider.calls.Count;
				long reenable = NanoTime();

				foreach (string proxy in L2Topology.ProxyPorts.Keys)
				{
					L2Topology.SetListener(proxy, true);
				}
				disabledProxies.Clear();

				Assert.IsTrue(L2Topology.WaitFor(() => client.Nodes.Length > 0, 5), "Client did not re-seed");
				long reseeded = NanoTime();
				long elapsed = (reseeded - reenable) / 1000000L;
				long budget = 2L * policy.tendInterval + policy.timeout;
				long refreshes = provider.CallsBetween(reenable, reseeded);

				Console.WriteLine("L2-N12-RESEED reenableToFirstNodeMs=" + elapsed + " tendInterval=" + policy.tendInterval +
					" connectTimeout=" + policy.timeout + " budgetMs=" + budget + " refreshesInWindow=" + refreshes +
					" refreshCallsBeforeReenable=" + callsBefore + " refreshCalls=" + provider.calls.Count +
					" refreshInterval=" + policy.discoveryRefreshIntervalMs + " nodes=" + Fmt(L2Topology.NodeNames(client)));
				Assert.AreEqual(0L, refreshes);
				Assert.AreEqual(1, provider.calls.Count);
				Assert.IsTrue(elapsed <= budget, "Re-seeded " + elapsed + "ms after re-enable, budget " + budget + "ms");

				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("N-12-after", client);
			}
			finally
			{
				client.Close();
			}
		}

		/// <summary>
		/// N-14: useServicesAlternate against a cluster with no alternate access address. The seed's
		/// service-clear-alt is empty and client initialization fails with an error that names the
		/// flag instead of presenting an empty cluster.
		/// </summary>
		[TestMethod]
		public void N14UseServicesAlternateWithoutAlternateFailsInit()
		{
			L2Node first = L2Topology.Nodes()[0];
			string alternate = Info.Request(L2Topology.Host, first.Port, "service-clear-alt");
			Console.WriteLine("L2-N14-SEED " + first.Name + " service-clear-alt=[" + (alternate ?? "") + "]");
			Assert.IsTrue(string.IsNullOrEmpty(alternate), alternate);

			ClientPolicy policy = L2Topology.Policy();
			policy.useServicesAlternate = true;

			Exception caught = null;

			try
			{
				new AerospikeClient(policy, L2Topology.Seed()).Close();
			}
			catch (Exception ex)
			{
				caught = ex;
			}

			Assert.IsNotNull(caught, "Expected init failure with useServicesAlternate and no alternate address");
			string message = Messages(caught);
			Console.WriteLine("L2-N14-ERROR " + caught.GetType().FullName + ": " + message);
			// Cluster.SeedNode wraps AlternateAddressMissing in AerospikeException.Connection.
			Assert.IsTrue(message.Contains("useServicesAlternate"), message);
			Assert.IsTrue(message.Contains(first.Name), message);
		}

		/// <summary>
		/// on_miss strict with an empty static-map (ipMap): every peer misses, the UX-2 error names the
		/// peer, its advertised endpoint, the standard list and the translator, and is repeated every
		/// tend. Only the seed is a <see cref="Node"/> and serves the keys it holds.
		/// </summary>
		[TestMethod]
		public void StrictEmptyIpMapDropsPeers()
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.ipMap = new Dictionary<string, string>();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertStrictMissDropsPeers("ipmap-empty-strict", policy, "static-map");
		}

		/// <summary>
		/// on_miss strict with a custom translator that maps nothing: as with the empty ipMap, named by
		/// the translator class.
		/// </summary>
		[TestMethod]
		public void StrictEmptyCustomTranslatorDropsPeers()
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new EmptyTranslator();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertStrictMissDropsPeers("custom-empty-strict", policy, typeof(EmptyTranslator).FullName);
		}

		private static void AssertStrictMissDropsPeers(string row, ClientPolicy policy, string translator)
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node seed = nodes[0];
			List<L2Node> peers = nodes.GetRange(1, nodes.Count - 1);

			CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(new List<string> {seed.Name}, L2Topology.NodeNames(client));

					foreach (L2Node peer in peers)
					{
						if (Misses(peer).Count < MISS_RETRIES)
						{
							return false;
						}
					}
					return true;
				});

				foreach (L2Node peer in peers)
				{
					List<string> misses = Misses(peer);
					Console.WriteLine("L2-MISS-RESULT " + row + " peer=" + peer.Name + " nodes=" + Fmt(L2Topology.NodeNames(client)) +
						" missWarnings=" + misses.Count);
					Assert.IsTrue(misses.Count >= MISS_RETRIES, "Missed peer " + peer.Name + " not retried every tend: " + Fmt(Warnings));

					string message = misses[0];
					Console.WriteLine("L2-MISS-LOG " + row + ' ' + message);

					foreach (string part in new string[] {peer.Name, peer.Advertised[0], "standard", translator, "pass_through"})
					{
						Assert.IsTrue(message.Contains(part), part + " not in: " + message);
					}
				}
				CollectionAssert.AreEqual(new List<string> {seed.Name}, L2Topology.NodeNames(client));
				AssertSeedKeysWork(row, client, seed);
			}
		}

		internal static void AssertSeedKeysWork(string row, AerospikeClient client, L2Node seed)
		{
			Cluster cluster = client.Cluster;
			WritePolicy policy = new WritePolicy(client.WritePolicyDefault);
			int onSeed = 0;
			int notOwned = 0;

			for (int i = 0; i < MISS_KEY_COUNT; i++)
			{
				Key key = new Key(L2Topology.Namespace, SET, "miss-" + i);
				Node node;

				try
				{
					node = Partition.Write(cluster, policy, key).GetNodeWrite(cluster);
				}
				catch (AerospikeException.InvalidNode)
				{
					notOwned++;
					continue;
				}

				Assert.AreEqual(seed.Name, node.Name);
				client.Put(policy, key, new Bin("v", i));
				Assert.AreEqual(i, client.Get(null, key).GetInt("v"));
				onSeed++;
			}
			Console.WriteLine("L2-MISS-KEYS " + row + " seed=" + seed.Name + " keysOnSeed=" + onSeed + " keysNotOwned=" + notOwned);
			Assert.IsTrue(onSeed > 0);
			Assert.IsTrue(notOwned > 0);
		}

		/// <summary>
		/// Return the warnings that a certificate matched the tls-name only by the deprecated CN
		/// fallback.
		/// </summary>
		internal static List<string> CnFallbackWarnings()
		{
			List<string> list = new List<string>();

			foreach (string warning in Warnings)
			{
				if (warning.Contains("matched the server certificate only by the legacy rule"))
				{
					list.Add(warning);
				}
			}
			return list;
		}

		internal static void CaptureWarnings()
		{
			Warnings.Clear();
			Errors.Clear();
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN)
				{
					Warnings.Enqueue(message);
				}
				else if (level == Log.Level.ERROR)
				{
					Errors.Enqueue(message);
				}
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
			});
		}

		private static void AssertPortMapJoins(string row, Dictionary<string, string> portMap)
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(portMap);
			Console.WriteLine("L2-PORT-MAP " + row + ' ' + FmtMap(portMap));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Console.WriteLine("L2-JOIN " + row + " nodes=" + Fmt(L2Topology.NodeNames(client)));
				Dictionary<string, int> owners = L2Topology.Owners(L2Topology.AssertAllPartitionsOwned(row, client));
				CollectionAssert.AreEqual(L2Topology.NodeNames(), Sorted(owners.Keys));
			}
		}

		private static void AssertOwnedKeysWork(AerospikeClient client, List<string> mappedNames, L2Node victim)
		{
			Node[] masters = client.Cluster.partitionMap[L2Topology.Namespace].replicas[0];
			WritePolicy policy = new WritePolicy(client.WritePolicyDefault);
			int noMaster = 0;
			int owned = 0;
			int missedKeys = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = masters[i];

				if (node == null)
				{
					noMaster++;
				}
				else
				{
					Assert.IsTrue(mappedNames.Contains(node.Name), node.Name);
				}
			}

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(L2Topology.Namespace, SET, "n8-" + i);

				if (masters[(int)Partition.GetPartitionId(key.digest)] == null)
				{
					missedKeys++;
				}
				else
				{
					owned++;
				}
				client.Put(policy, key, new Bin("v", i));
				Assert.AreEqual(i, client.Get(null, key).GetInt("v"));
			}
			Console.WriteLine("L2-N8-KEYS victim=" + victim.Name + " partitionsWithoutMaster=" + noMaster +
				" keysOnMappedMasters=" + owned + " keysOnMissedPartitions=" + missedKeys);
			Assert.IsTrue(noMaster > 0);
			Assert.IsTrue(owned > 0);
			Assert.IsTrue(missedKeys > 0);
		}

		internal static List<string> Misses(L2Node victim)
		{
			List<string> misses = new List<string>();

			foreach (string warning in Warnings)
			{
				if (warning.Contains("has no mapping for node " + victim.Name))
				{
					misses.Add(warning);
				}
			}
			return misses;
		}

		private static bool Writes(AerospikeClient client, WritePolicy policy, Key key)
		{
			try
			{
				client.Put(policy, key, new Bin("v", 0));
				return true;
			}
			catch (AerospikeException)
			{
				return false;
			}
		}

		internal static void SkipUnless(bool condition, string reason)
		{
			if (!condition)
			{
				Console.WriteLine("L2-SKIP " + reason);
				Assert.Inconclusive(reason);
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

		internal static string Messages(Exception t)
		{
			StringBuilder sb = new StringBuilder();

			for (Exception e = t; e != null; e = e.InnerException)
			{
				if (sb.Length > 0)
				{
					sb.Append(" <- ");
				}
				sb.Append(e.Message);
			}
			return sb.ToString();
		}

		/// <summary>
		/// Monotonic clock in nanoseconds, the time base of the <c>StartNanos</c> of an
		/// <see cref="L2Topology.Outcome"/>.
		/// </summary>
		internal static long NanoTime()
		{
			return (long)(Stopwatch.GetTimestamp() * (1000000000.0 / Stopwatch.Frequency));
		}

		/// <summary>
		/// Return the value for a key or null, as Java's Map.get.
		/// </summary>
		internal static string Get(Dictionary<string, string> map, string key)
		{
			return map != null && map.TryGetValue(key, out string value) ? value : null;
		}

		internal static List<string> Sorted(IEnumerable<string> values)
		{
			List<string> list = new List<string>(values);
			list.Sort(StringComparer.Ordinal);
			return list;
		}

		internal static string Fmt(IEnumerable values)
		{
			StringBuilder sb = new StringBuilder("[");

			foreach (object value in values)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(value);
			}
			return sb.Append(']').ToString();
		}

		internal static string FmtMap<K, V>(IDictionary<K, V> map)
		{
			StringBuilder sb = new StringBuilder("{");

			foreach (KeyValuePair<K, V> entry in map)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(entry.Key).Append('=').Append(entry.Value);
			}
			return sb.Append('}').ToString();
		}
	}
}
