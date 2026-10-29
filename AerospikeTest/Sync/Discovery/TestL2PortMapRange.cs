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
using Aerospike.Client;
using Joined = Aerospike.Test.L2Topology.Joined;
using L2Node = Aerospike.Test.L2Topology.L2Node;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3 row N-19 against the port-map-range profile of
	/// test/discovery/run_l2.sh. The nodes sit on a fixed subnet and DISCOVERY_PORT_MAP_RANGE names a
	/// contiguous range of node addresses, including addresses no node has yet. Toxiproxy has a
	/// listener for every address in the range on DISCOVERY_PORT_MAP_PORT_BASE plus the address's last
	/// octet, upstream by IP. One port-map built from that rule before the cluster scales out covers
	/// every node started inside the range. Without the profile every case is skipped.
	/// </summary>
	[TestClass]
	public class TestL2PortMapRange
	{
		private const string BLOCKER = " needs DISCOVERY_PORT_MAP_RANGE, DISCOVERY_PORT_MAP_PORT_BASE and " +
			"DISCOVERY_CONTROL_DIR, nodes on a fixed subnet with a listener per address of a range " +
			"(run_l2.sh L2_PROFILE=port-map-range)";
		private const int MISS_RETRIES = 3;

		private readonly List<string> addedNodes = new List<string>();

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

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
		/// N-19: a port-map pre-filled from a fixed rule (node address to listener port base plus the
		/// address's last octet) for every address of the range joins the cluster with no warning. A
		/// node started on a free address of the range is an active <see cref="Node"/> within 2 x
		/// tend_interval of the peers-generation bump that announces it, with no client configuration
		/// change, and the partition map is then complete with the new node among the owners.
		/// </summary>
		[TestMethod]
		public void N19PrefilledPortMapJoinsNodeInRange()
		{
			RequireRange();

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			List<string> names = L2Topology.NodeNames();
			Dictionary<string, string> portMap = RangePortMap(nodes);
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(portMap);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, names);
				L2Topology.AssertAllPartitionsOwned("N-19-before", client);
				Assert.AreEqual(0, TestL2Topology.Warnings.Count, TestL2Topology.Fmt(TestL2Topology.Warnings));

				string address = FreeAddress(nodes);
				long generation = L2Topology.PeersGeneration(first);
				Dictionary<string, string> ack = Start(address);
				Assert.AreEqual(address, ack["address"]);

				Joined joined = L2Topology.WaitJoined(client, first, generation, names);
				Node node = joined.Node;
				long budget = 2L * policy.tendInterval;
				int port = ListenerPort(address);
				Dictionary<string, string> info = Info.Request(L2Topology.Host, port, "node", "service-clear-std");
				portMap.TryGetValue(address + ':' + ServicePort(nodes), out string mapped);
				Console.WriteLine("L2-N19-TIMING node=" + node.Name + " host=" + node.Host + " container=" +
					ack["container"] + " address=" + address + " advertised=" + TestL2Topology.Get(info, "service-clear-std") +
					" mapped=" + mapped + " generation=" + generation + "->" +
					joined.Generation + " bumpToActiveMs=" + joined.BumpToActiveMs + " tendInterval=" + policy.tendInterval +
					" budgetMs=" + budget);
				Assert.AreEqual(TestL2Topology.Get(info, "node"), node.Name);
				Assert.AreEqual(port, node.Host.port);
				Assert.IsTrue(joined.BumpToActiveMs <= budget,
					node.Name + " active " + joined.BumpToActiveMs + "ms after the bump, budget " + budget + "ms");

				bool owns = L2Topology.WaitFor(() => L2Topology.Owners(client).ContainsKey(node.Name));
				Console.WriteLine("L2-N19-OWNERS " + TestL2Topology.FmtMap(L2Topology.Owners(client)));
				Assert.IsTrue(owns, node.Name + " owns no partitions");
				Assert.AreEqual(names.Count + 1, L2Topology.Owners(L2Topology.AssertAllPartitionsOwned("N-19-after", client)).Count);
			}
		}

		/// <summary>
		/// N-19 outside the range: a node started on an address the pre-filled port-map does not cover
		/// is decided by on_miss. With strict the UX-2 error names the node, its advertised endpoint and
		/// the translator on every tend and no <see cref="Node"/> is created. With the default the unchanged
		/// address, which the client cannot reach, is tried on every tend and no <see cref="Node"/> is created.
		/// </summary>
		[TestMethod]
		public void N19NodeOutsideRangeDecidedByOnMiss()
		{
			RequireRange();

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			List<string> names = L2Topology.NodeNames();
			Dictionary<string, string> portMap = RangePortMap(nodes);
			string[] range = Range();
			string address = Prefix(range[1]) + (Octet(range[1]) + 1);
			string advertised = address + ':' + ServicePort(nodes);

			Assert.AreEqual(address, Start(address)["address"]);
			L2Topology.WaitClusterStable(nodes.Count + 1);

			string name = L2Topology.PeerName(first, "peers-clear-std", advertised);
			Console.WriteLine("L2-N19-OUTSIDE node=" + name + " advertised=" + advertised + " range=" + L2Topology.PortMapRange);
			Assert.IsNotNull(name, "No peer advertises " + advertised);
			Assert.IsFalse(portMap.ContainsKey(advertised));

			ClientPolicy strict = L2Topology.Policy();
			strict.addressTranslator = new PortMapAddressTranslator(portMap);
			strict.translationOnMiss = TranslationMissMode.STRICT;
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(strict, L2Topology.Seed()))
			{
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
					return Misses(name).Count >= MISS_RETRIES;
				});

				List<string> misses = Misses(name);
				Console.WriteLine("L2-N19-STRICT node=" + name + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " missWarnings=" +
					misses.Count);
				Assert.IsTrue(misses.Count >= MISS_RETRIES,
					"Missed node " + name + " not retried every tend: " + TestL2Topology.Fmt(TestL2Topology.Warnings));
				CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));

				string message = misses[0];
				Console.WriteLine("L2-N19-STRICT-LOG " + message);
				Endpoint endpoint = new Endpoint(name, null, address, ServicePort(nodes), Endpoint.SourceList.STANDARD);
				string expected = new AerospikeException.TranslationMiss(endpoint, "port-map").BaseMessage;
				Assert.IsTrue(message.Contains(expected), message + " does not contain " + expected);
			}

			ClientPolicy passThrough = L2Topology.Policy();
			passThrough.addressTranslator = new PortMapAddressTranslator(portMap);
			Assert.AreEqual(TranslationMissMode.PASS_THROUGH, passThrough.translationOnMiss);
			TestL2Topology.CaptureWarnings();

			using (AerospikeClient client = new AerospikeClient(passThrough, L2Topology.Seed()))
			{
				string tried = "Add node " + address + ' ' + ServicePort(nodes) + " failed";
				L2Topology.WaitFor(() =>
				{
					CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
					return Attempts(tried).Count >= MISS_RETRIES;
				});

				List<string> attempts = Attempts(tried);
				Console.WriteLine("L2-N19-PASS-THROUGH node=" + name + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) +
					" attempts=" + attempts.Count + " missWarnings=" + Misses(name).Count);
				Assert.IsTrue(attempts.Count >= MISS_RETRIES,
					"Unchanged address " + advertised + " not tried every tend: " + TestL2Topology.Fmt(TestL2Topology.Warnings));
				Console.WriteLine("L2-N19-PASS-THROUGH-LOG " + attempts[0]);
				CollectionAssert.AreEqual(names, L2Topology.NodeNames(client));
				Assert.AreEqual(0, Misses(name).Count, TestL2Topology.Fmt(Misses(name)));
			}
		}

		private Dictionary<string, string> Start(string address)
		{
			Dictionary<string, string> ack = L2Topology.Control("start address=" + address);
			Assert.IsNotNull(TestL2Topology.Get(ack, "container"));
			addedNodes.Add(ack["container"]);
			return ack;
		}

		private static void RequireRange()
		{
			TestL2Topology.SkipUnless(L2Topology.PortMapRange.Length > 0 && L2Topology.PortMapPortBase.Length > 0 &&
				L2Topology.ControlDir.Length > 0, "N-19" + BLOCKER);
		}

		/// <summary>
		/// Build the port-map from the rule, for every address of the range, and check that the
		/// cluster's nodes are inside the range and every listener of the range exists.
		/// </summary>
		private static Dictionary<string, string> RangePortMap(List<L2Node> nodes)
		{
			string[] range = Range();
			string prefix = Prefix(range[0]);
			int port = ServicePort(nodes);
			HashSet<int> listeners = L2Topology.ListenerPorts();
			Dictionary<string, string> map = new Dictionary<string, string>();

			for (int octet = Octet(range[0]); octet <= Octet(range[1]); octet++)
			{
				int listener = ListenerPort(prefix + octet);
				Assert.IsTrue(listeners.Contains(listener),
					"No listener on " + listener + " for " + prefix + octet + ": " + TestL2Topology.Fmt(listeners));
				map[prefix + octet + ':' + port] = L2Topology.Listener(listener);
			}

			foreach (L2Node node in nodes)
			{
				map.TryGetValue(node.Advertised[0], out string mapped);
				Console.WriteLine("L2-N19-NODE " + node.Name + " advertised=" + TestL2Topology.Fmt(node.Advertised) + " mapped=" + mapped);
				Assert.IsTrue(map.ContainsKey(node.Advertised[0]),
					TestL2Topology.Fmt(node.Advertised) + " outside " + L2Topology.PortMapRange);
			}
			Console.WriteLine("L2-N19-PORT-MAP range=" + L2Topology.PortMapRange + " portBase=" + L2Topology.PortMapPortBase +
				" entries=" + map.Count + ' ' + TestL2Topology.FmtMap(map));
			return map;
		}

		private static string FreeAddress(List<L2Node> nodes)
		{
			string[] range = Range();
			string prefix = Prefix(range[0]);
			int port = ServicePort(nodes);
			List<string> used = new List<string>();

			foreach (L2Node node in nodes)
			{
				used.AddRange(node.Advertised);
			}

			for (int octet = Octet(range[0]); octet <= Octet(range[1]); octet++)
			{
				if (!used.Contains(prefix + octet + ':' + port))
				{
					return prefix + octet;
				}
			}
			throw new InvalidOperationException("No free address in " + L2Topology.PortMapRange + ": " + TestL2Topology.Fmt(used));
		}

		private static List<string> Misses(string nodeName)
		{
			List<string> list = new List<string>();

			foreach (string warning in TestL2Topology.Warnings)
			{
				if (warning.Contains("has no mapping for node " + nodeName))
				{
					list.Add(warning);
				}
			}
			return list;
		}

		private static List<string> Attempts(string tried)
		{
			List<string> list = new List<string>();

			foreach (string warning in TestL2Topology.Warnings)
			{
				// The client may prefix log messages, so match the attempt text anywhere in the message.
				if (warning.Contains(tried))
				{
					list.Add(warning);
				}
			}
			return list;
		}

		private static int ListenerPort(string address)
		{
			return int.Parse(L2Topology.PortMapPortBase) + Octet(address);
		}

		private static int ServicePort(List<L2Node> nodes)
		{
			string advertised = nodes[0].Advertised[0];
			return int.Parse(advertised.Substring(advertised.LastIndexOf(':') + 1));
		}

		private static string[] Range()
		{
			string[] range = L2Topology.PortMapRange.Split('-');
			Assert.AreEqual(2, range.Length, L2Topology.PortMapRange);
			Assert.AreEqual(Prefix(range[0]), Prefix(range[1]), L2Topology.PortMapRange);
			return range;
		}

		private static string Prefix(string address)
		{
			return address.Substring(0, address.LastIndexOf('.') + 1);
		}

		private static int Octet(string address)
		{
			return int.Parse(address.Substring(address.LastIndexOf('.') + 1));
		}
	}
}
