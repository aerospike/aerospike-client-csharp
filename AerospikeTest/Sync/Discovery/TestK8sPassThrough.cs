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
using Pod = Aerospike.Test.K8sCluster.Pod;

namespace Aerospike.Test
{
	/// <summary>
	/// Pass-through translation against an Aerospike cluster whose alternate access addresses the
	/// Aerospike Kubernetes Operator renders. The client seeds through <c>L3_SEED</c>, reads the alternate
	/// list with <see cref="PassThroughAddressTranslator"/>, and has no translation map, no port arithmetic
	/// and no seed provider: every peer comes from what the server advertises.
	/// </summary>
	[TestClass]
	public class TestK8sPassThrough
	{
		private const string Set = "l3-pass-through";
		private const int MinPods = 2;
		private const long RoutingWaitMs = 120000;
		private const int KeySearchLimit = 100000;
		private const string SharedEndpointProfile = " needs multiPodPerHost: true with every node labelled with " +
			"one endpoint hostname, the cluster setup-kind-ako builds (run_l3.sh default profile, or the discovery-k8s workflow)";
		private const string PerNodeProfile = " needs multiPodPerHost: false with each kind worker labelled with " +
			"its own endpoint hostname (run_l3.sh L3_PROFILE=per-node)";

		private static string ns;
		private static Host[] seeds;

		private K8sCluster scaled;
		private int scaledFrom;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			if (string.IsNullOrEmpty(K8sCluster.Kubeconfig))
			{
				Assert.Inconclusive("Kubernetes pass-through " + K8sCluster.Skip);
			}

			string seed = Environment.GetEnvironmentVariable("L3_SEED");
			Assert.IsFalse(string.IsNullOrEmpty(seed), "L3_SEED is required when DISCOVERY_KUBECONFIG is set");
			int index = seed.LastIndexOf(':');
			Assert.IsTrue(index > 0 && index < seed.Length - 1, "L3_SEED must be host:port, got: " + seed);
			seeds = new Host[] { new Host(seed.Substring(0, index), int.Parse(seed.Substring(index + 1))) };
			ns = !string.IsNullOrEmpty(SuiteHelpers.ns) ? SuiteHelpers.ns : "test";
		}

		[TestInitialize]
		public void RequireCluster()
		{
			if (string.IsNullOrEmpty(K8sCluster.Kubeconfig))
			{
				Assert.Inconclusive("Kubernetes pass-through " + K8sCluster.Skip);
			}
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (scaled != null)
			{
				K8sCluster cluster = scaled;
				scaled = null;
				cluster.Scale(scaledFrom);
			}
		}

		/// <summary>
		/// Every node carries the same endpoint hostname as its configured-alternate-access-address
		/// label, so the operator advertises that hostname with each pod's own Service NodePort as
		/// alternate-access-port.
		/// </summary>
		[TestMethod]
		public void SharedEndpointAdvertisesEachPodNodePortAndRoutesEveryPartition()
		{
			K8sCluster cluster = K8sCluster.Find();
			SkipUnless(cluster.MultiPodPerHost(), "Shared endpoint pass-through" + SharedEndpointProfile);
			Assert.IsFalse(string.IsNullOrEmpty(K8sCluster.EndpointHostname), "DISCOVERY_ENDPOINT_HOSTNAME is not set");
			Assert.AreEqual("configuredIP", cluster.Get(".spec.aerospikeNetworkPolicy.alternateAccess"));

			int size = cluster.Size();

			if (size < MinPods)
			{
				scaled = cluster;
				scaledFrom = size;
				cluster.Scale(MinPods);
			}

			int port = cluster.ServicePort();
			List<Pod> pods = cluster.Pods();
			Dictionary<string, string> expected = new Dictionary<string, string>();
			HashSet<int> nodePorts = new HashSet<int>();

			foreach (Pod pod in pods)
			{
				string label = K8sCluster.NodeLabel(pod.Node);
				int nodePort = cluster.NodePort(pod.Name, port);
				Console.WriteLine("L3-POD " + pod + " label=" + label + " serviceNodePort=" + nodePort);
				Assert.AreEqual(K8sCluster.EndpointHostname, label, pod.Node + " label");
				Assert.IsTrue(nodePort > 0, pod.Name + " has no Service NodePort for " + port);
				nodePorts.Add(nodePort);
				expected[pod.Name] = K8sCluster.EndpointHostname + ':' + nodePort;
			}
			Assert.AreEqual(pods.Count, nodePorts.Count, "Distinct NodePorts " + FormatInts(nodePorts));
			Assert.IsTrue(nodePorts.IsSupersetOf(K8sCluster.ServiceNodePorts),
				"DISCOVERY_SERVICE_NODE_PORTS " + FormatInts(K8sCluster.ServiceNodePorts) + " not in " + FormatInts(nodePorts));
			AssertRoutesEveryPartition("shared-endpoint", pods, expected);
		}

		/// <summary>
		/// Each kind worker carries its own hostname as its configured-alternate-access-address label
		/// and runs one pod, so the operator advertises the worker's hostname with the pod's hostPort.
		/// </summary>
		[TestMethod]
		public void PerNodeEndpointsAdvertiseEachWorkerHostnameAndRouteEveryPartition()
		{
			K8sCluster cluster = K8sCluster.Find();
			SkipUnless(!cluster.MultiPodPerHost(), "Per-node pass-through" + PerNodeProfile);
			Assert.AreEqual("configuredIP", cluster.Get(".spec.aerospikeNetworkPolicy.alternateAccess"));

			int port = cluster.ServicePort();
			List<Pod> pods = cluster.Pods();
			Assert.IsTrue(pods.Count >= MinPods, "Pods " + FormatPods(pods));
			Dictionary<string, string> expected = new Dictionary<string, string>();
			HashSet<string> workers = new HashSet<string>();
			HashSet<string> labels = new HashSet<string>();

			foreach (Pod pod in pods)
			{
				string label = K8sCluster.NodeLabel(pod.Node);
				int hostPort = cluster.HostPort(pod.Name, port);
				Console.WriteLine("L3-POD " + pod + " label=" + label + " nodeAddress=" + K8sCluster.NodeAddress(pod.Node) +
					" hostPort=" + hostPort + " serviceNodePort=" + cluster.NodePort(pod.Name, port));
				Assert.IsFalse(string.IsNullOrEmpty(label), pod.Node + " has no " + K8sCluster.AlternateAccessLabel + " label");
				Assert.IsTrue(hostPort > 0, pod.Name + " has no hostPort for " + port);
				workers.Add(pod.Node);
				labels.Add(label);
				expected[pod.Name] = label + ':' + hostPort;
			}
			Assert.AreEqual(pods.Count, workers.Count, "Workers " + Format(workers));
			Assert.AreEqual(pods.Count, labels.Count, "Labels " + Format(labels));
			AssertRoutesEveryPartition("per-node", pods, expected);
		}

		private static void AssertRoutesEveryPartition(string shape, List<Pod> pods, Dictionary<string, string> expected)
		{
			ClientPolicy policy = Policy();
			CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, seeds))
			{
				Node[] masters = new Node[Node.PARTITIONS];
				bool routed = K8sCluster.WaitFor(() => client.Nodes.Length == pods.Count &&
					Unowned(client, masters) == 0 && Owners(masters).Count == pods.Count, RoutingWaitMs, 500);
				int unowned = Unowned(client, masters);
				Dictionary<string, int> ownerCounts = Owners(masters);
				Console.WriteLine("L3-ROUTING shape=" + shape + " seeds=" + FormatSeeds(seeds) + " pods=" + pods.Count +
					" nodes=" + client.Nodes.Length + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
					" owners=" + TestL2Topology.FmtMap(ownerCounts));
				Assert.IsTrue(routed, "Client did not hold " + pods.Count + " nodes owning every partition within " +
					RoutingWaitMs + "ms");

				SortedDictionary<string, Pod> podsByIp = new SortedDictionary<string, Pod>(StringComparer.Ordinal);

				foreach (Pod pod in pods)
				{
					podsByIp[pod.Ip] = pod;
				}

				HashSet<string> matched = new HashSet<string>();
				Dictionary<string, string> seededAt = new Dictionary<string, string>();

				foreach (Node node in client.Nodes)
				{
					Dictionary<string, string> info = Info.Request(null, node, "service-clear-std", "service-clear-alt");
					Pod pod = PodOf(podsByIp, Get(info, "service-clear-std"));
					string advertised = Get(info, "service-clear-alt");
					Host h = node.Host;
					Console.WriteLine("L3-NODE shape=" + shape + " node=" + node.Name + " pod=" +
						(pod == null ? "null" : pod.Name) + " worker=" + (pod == null ? "null" : pod.Node) + " access=" +
						Get(info, "service-clear-std") + " alternate=" + advertised + " expected=" +
						(pod == null ? "null" : expected[pod.Name]) + " clientHost=" + h.name + ':' + h.port);
					Assert.IsNotNull(pod, node.Name + " access " + Get(info, "service-clear-std") + " is no pod IP in " +
						Format(podsByIp.Keys));
					Assert.IsTrue(matched.Add(pod.Name), pod.Name + " matched twice");
					Assert.AreEqual(expected[pod.Name], advertised, node.Name + " (" + pod.Name + ") advertised alternate address");

					if (IsAddress(seeds, h))
					{
						seededAt[node.Name] = advertised;
					}
					else
					{
						AssertAddress(advertised, h);
					}
					Assert.AreSame(node, client.Cluster.GetNode(node.Name));
				}
				CollectionAssert.AreEquivalent(expected.Keys.ToList(), matched.ToList());
				AssertServedByEveryNode(shape, client, masters);

				foreach (KeyValuePair<string, string> entry in seededAt)
				{
					AssertReachableAtAdvertised(shape, policy, entry.Key, entry.Value);
				}
			}
		}

		private static void AssertReachableAtAdvertised(string shape, ClientPolicy policy, string name, string advertised)
		{
			int index = advertised.LastIndexOf(':');
			Host seed = new Host(advertised.Substring(0, index), int.Parse(advertised.Substring(index + 1)));

			using (AerospikeClient client = new AerospikeClient(policy, seed))
			{
				Node node = client.Cluster.GetNode(name);
				Host h = node.Host;
				Console.WriteLine("L3-ADVERTISED shape=" + shape + " node=" + name + " seed=" + advertised +
					" clientHost=" + h.name + ':' + h.port);
				AssertAddress(advertised, h);
				Assert.IsTrue(node.Active, name + " at " + advertised + " is not active");
			}
		}

		private static void AssertServedByEveryNode(string shape, AerospikeClient client, Node[] masters)
		{
			Cluster cluster = client.Cluster;
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			wp.replica = Replica.MASTER;
			Policy rp = new Policy(client.ReadPolicyDefault);
			rp.replica = Replica.MASTER;

			foreach (Node node in client.Nodes)
			{
				Key key = null;

				for (int i = 0; i < KeySearchLimit && key == null; i++)
				{
					Key candidate = new Key(ns, Set, shape + '-' + i);

					if (ReferenceEquals(masters[(int)Partition.GetPartitionId(candidate.digest)], node))
					{
						key = candidate;
					}
				}
				Assert.IsNotNull(key, node.Name + " is master of no partition");
				Assert.AreSame(node, Partition.Write(cluster, wp, key).GetNodeWrite(cluster));
				Assert.AreSame(node, Partition.Read(cluster, rp, key).GetNodeRead(cluster));
				client.Put(wp, key, new Bin("v", node.Name));
				Assert.AreEqual(node.Name, client.Get(rp, key).GetString("v"));
				Console.WriteLine("L3-SERVED shape=" + shape + " node=" + node.Name + " key=" + key.userKey +
					" partition=" + Partition.GetPartitionId(key.digest) + " write=ok read=ok");
			}
		}

		private static int Unowned(AerospikeClient client, Node[] masters)
		{
			Partitions partitions = client.Cluster.partitionMap[ns];
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = null;

				if (partitions != null)
				{
					node = partitions.replicas[0][i];
				}

				if (node == null || !node.Active || !ReferenceEquals(client.Cluster.GetNode(node.Name), node))
				{
					masters[i] = null;
					unowned++;
				}
				else
				{
					masters[i] = node;
				}
			}
			return unowned;
		}

		private static Dictionary<string, int> Owners(Node[] masters)
		{
			SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Node node in masters)
			{
				if (node != null)
				{
					owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
				}
			}
			return new Dictionary<string, int>(owners);
		}

		private static Pod PodOf(SortedDictionary<string, Pod> podsByIp, string access)
		{
			if (access == null)
			{
				return null;
			}

			foreach (Host h in Host.ParseServiceHosts(access))
			{
				if (podsByIp.TryGetValue(h.name, out Pod pod))
				{
					return pod;
				}
			}
			return null;
		}

		private static void AssertAddress(string advertised, Host h)
		{
			int index = advertised.LastIndexOf(':');
			Host host = new Host(advertised.Substring(0, index), int.Parse(advertised.Substring(index + 1)));
			Assert.IsTrue(IsAddress(new Host[] { host }, h),
				h.name + ':' + h.port + " is not " + host.name + Format(Addresses(host.name)) + ':' + host.port);
		}

		private static bool IsAddress(Host[] hosts, Host h)
		{
			foreach (Host host in hosts)
			{
				if (host.port == h.port && (host.name.Equals(h.name) || Addresses(host.name).Contains(h.name)))
				{
					return true;
				}
			}
			return false;
		}

		private static List<string> Addresses(string name)
		{
			List<string> list = new List<string>();

			foreach (IPAddress address in Dns.GetHostAddresses(name))
			{
				list.Add(address.ToString());
			}
			return list;
		}

		private static ClientPolicy Policy()
		{
			string password = Environment.GetEnvironmentVariable("AEROSPIKE_ADMIN_PASSWORD");
			Assert.IsFalse(string.IsNullOrEmpty(password), "AEROSPIKE_ADMIN_PASSWORD is required");
			ClientPolicy policy = new ClientPolicy();
			policy.user = "admin";
			policy.password = password;
			policy.authMode = AuthMode.INTERNAL;
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			return policy;
		}

		private static void CaptureLog()
		{
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) => Console.WriteLine("CLIENT-LOG " + level + ' ' + message));
		}

		private static void SkipUnless(bool condition, string reason)
		{
			if (!condition)
			{
				Console.WriteLine("L3-SKIP " + reason);
				Assert.Inconclusive(reason);
			}
		}

		private static string Get(Dictionary<string, string> info, string key)
		{
			return info.TryGetValue(key, out string value) ? value : null;
		}

		private static string Format(IEnumerable<string> values)
		{
			return TestL2Topology.Fmt(values);
		}

		private static string FormatInts(IEnumerable<int> values)
		{
			List<string> parts = new List<string>();

			foreach (int value in values)
			{
				parts.Add(value.ToString());
			}
			return Format(parts);
		}

		private static string FormatPods(List<Pod> pods)
		{
			List<string> parts = new List<string>();

			foreach (Pod pod in pods)
			{
				parts.Add(pod.ToString());
			}
			return Format(parts);
		}

		private static string FormatSeeds(Host[] hosts)
		{
			List<string> parts = new List<string>();

			foreach (Host host in hosts)
			{
				parts.Add(host.name + ':' + host.port);
			}
			return Format(parts);
		}
	}
}
