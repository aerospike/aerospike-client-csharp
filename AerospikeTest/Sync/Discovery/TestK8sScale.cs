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
using System.Diagnostics;
using Aerospike.Client;
using Pod = Aerospike.Test.K8sCluster.Pod;

namespace Aerospike.Test
{
	/// <summary>
	/// Scaling the AerospikeCluster under a running pass-through client, and the per-pod Service
	/// NodePorts across pod rescheduling and recreation, on the shared endpoint the operator renders:
	/// every pod advertises the endpoint hostname with its own Service NodePort as alternate address.
	/// </summary>
	[TestClass]
	public class TestK8sScale
	{
		private const string Set = "l3-scale";
		private const int MinPods = 2;
		private const long RoutingWaitMs = 120000;
		private const int LoadThreads = 4;
		private const int LoadKeys = 1000;
		private const int QuietTendIntervals = 5;
		private const int KeySearchLimit = 100000;
		private const string SharedEndpointProfile = " needs multiPodPerHost: true with every node labelled with " +
			"one endpoint hostname, the cluster setup-kind-ako builds (run_l3.sh default profile, or the discovery-k8s workflow)";

		private static string ns;
		private static Host[] seeds;

		private K8sCluster cluster;
		private int foundSize;
		private string pinnedService;
		private string heldService;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			if (string.IsNullOrEmpty(K8sCluster.Kubeconfig))
			{
				Assert.Inconclusive("Kubernetes scale " + K8sCluster.Skip);
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
				Assert.Inconclusive("Kubernetes scale " + K8sCluster.Skip);
			}

			K8sCluster found = K8sCluster.Find();
			if (!found.MultiPodPerHost())
			{
				Assert.Inconclusive("Kubernetes scale" + SharedEndpointProfile);
			}
			Assert.IsFalse(string.IsNullOrEmpty(K8sCluster.EndpointHostname), "DISCOVERY_ENDPOINT_HOSTNAME is not set");
			Assert.AreEqual("configuredIP", found.Get(".spec.aerospikeNetworkPolicy.alternateAccess"));
			cluster = found;
			foundSize = found.Size();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (cluster == null)
			{
				return;
			}

			K8sCluster c = cluster;
			cluster = null;

			try
			{
				if (heldService != null)
				{
					c.DeleteService(heldService);
					heldService = null;
				}

				if (pinnedService != null)
				{
					c.DeleteService(pinnedService);
					Console.WriteLine("L3-PINNED-DELETED service=" + pinnedService);
					pinnedService = null;
				}
			}
			finally
			{
				if (c.Size() != foundSize || c.Pods().Count != foundSize)
				{
					c.Scale(foundSize);
				}
			}
		}

		[TestMethod]
		public void ScaleUpNewNodeActiveWithinTwoTendIntervals()
		{
			int size = foundSize;
			int port = cluster.ServicePort();
			HashSet<string> before = PodNames(cluster.Pods());
			ClientPolicy policy = Policy();
			Assert.IsNull(policy.seedCandidateProvider);

			using (AerospikeClient client = new AerospikeClient(policy, seeds))
			{
				AwaitRouting("scale-up-before", client, size);

				Dictionary<string, Node> existing = new Dictionary<string, Node>();

				foreach (Node node in client.Nodes)
				{
					existing[node.Name] = node;
				}

				Node observer = client.Nodes[0];
				long generation = PeersGeneration(observer);
				long bumpTicks = 0;
				long bumpAt = 0;
				long activeTicks = 0;
				Node added = null;

				cluster.Resize(size + 1);
				bool seen = K8sCluster.WaitFor(() =>
				{
					long now = Stopwatch.GetTimestamp();

					if (bumpTicks == 0 && PeersGeneration(observer) != generation)
					{
						bumpTicks = now;
						bumpAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
					}

					if (activeTicks == 0)
					{
						foreach (Node node in client.Nodes)
						{
							if (node.Active && !existing.ContainsKey(node.Name))
							{
								activeTicks = now;
								added = node;
							}
						}
					}
					return bumpTicks != 0 && activeTicks != 0;
				}, K8sCluster.ScaleTimeoutMs, 5);
				Assert.IsTrue(seen, "No peers-generation bump on " + observer.Name + " or no new active node");

				long elapsed = (long)((activeTicks - bumpTicks) * 1000.0 / Stopwatch.Frequency);
				long budget = 2L * policy.tendInterval;
				cluster.Scale(size + 1);

				Pod pod = null;

				foreach (Pod p in cluster.Pods())
				{
					if (!before.Contains(p.Name))
					{
						pod = p;
					}
				}
				Assert.IsNotNull(pod, "No new pod in " + FormatPods(cluster.Pods()));

				int nodePort = cluster.NodePort(pod.Name, port);
				Node nodeAdded = added;
				Dictionary<string, string> info = Info.Request(null, nodeAdded, "service-clear-std", "service-clear-alt");
				Host h = nodeAdded.Host;
				Console.WriteLine("L3-SCALE-UP node=" + nodeAdded.Name + " pod=" + pod.Name + " access=" +
					Get(info, "service-clear-std") + " alternate=" + Get(info, "service-clear-alt") + " serviceNodePort=" +
					nodePort + " clientHost=" + h.name + ':' + h.port + " generation=" + generation + "->" +
					PeersGeneration(observer) + " bumpToActiveMs=" + elapsed + " tendInterval=" + policy.tendInterval +
					" budgetMs=" + budget + " bumpAt=" + DateTimeOffset.FromUnixTimeMilliseconds(bumpAt) +
					" podReadyAt=" + cluster.PodReadyAt(pod.Name) + " readinessProbe=" + cluster.ReadinessProbe(pod.Name) +
					" publishNotReadyAddresses=" + cluster.PublishNotReadyAddresses(pod.Name));
				Assert.IsTrue(nodePort > 0, pod.Name + " has no Service NodePort for " + port);
				Assert.IsTrue(HasAddress(Get(info, "service-clear-std"), pod.Ip),
					nodeAdded.Name + " access " + Get(info, "service-clear-std") + " is not " + pod);
				Assert.AreEqual(K8sCluster.EndpointHostname + ':' + nodePort, Get(info, "service-clear-alt"),
					nodeAdded.Name + " (" + pod.Name + ") advertised alternate address");
				Assert.AreEqual(nodePort, h.port);
				Assert.IsTrue(elapsed <= budget,
					nodeAdded.Name + " active " + elapsed + "ms after the bump, budget " + budget + "ms");

				AwaitRouting("scale-up-after", client, size + 1);

				foreach (KeyValuePair<string, Node> entry in existing)
				{
					Assert.AreSame(entry.Value, client.Cluster.GetNode(entry.Key), entry.Key + " replaced");
				}
				AssertServedByEveryNode("scale-up", client);
			}
		}

		[TestMethod]
		public void ScaleDownRemovesNodeWithoutRetryStorm()
		{
			if (foundSize < MinPods)
			{
				cluster.Scale(MinPods);
			}

			int size = cluster.Size();
			List<Pod> pods = cluster.Pods();
			Pod removed = HighestOrdinal(pods);
			ClientPolicy policy = Policy();

			using (AerospikeClient client = new AerospikeClient(policy, seeds))
			{
				AwaitRouting("scale-down-before", client, size);

				Node victim = NodeOf(client, removed);
				Assert.IsNotNull(victim, "No node for " + removed);
				string victimName = victim.Name;
				Console.WriteLine("L3-SCALE-DOWN-VICTIM node=" + victimName + " pod=" + removed);

				using (Load load = new Load(client))
				{
					Assert.IsTrue(K8sCluster.WaitFor(() => load.Succeeded > 0, RoutingWaitMs, 10), "Load did not start");

					long failedBefore = load.Failed;
					long start = Environment.TickCount64;
					cluster.Scale(size - 1);

					Node[] masters = new Node[Node.PARTITIONS];
					bool gone = K8sCluster.WaitFor(() => !Names(client).Contains(victimName) &&
						!InPartitionMap(client, victimName) && client.Nodes.Length == size - 1 &&
						Unowned(client, masters) == 0, RoutingWaitMs, 50);
					long transitionMs = Environment.TickCount64 - start;
					long transitionFailed = load.Failed - failedBefore;
					Console.WriteLine("L3-SCALE-DOWN-TRANSITION node=" + victimName + " pod=" + removed.Name +
						" transitionMs=" + transitionMs + " failedCommands=" + transitionFailed + " succeededCommands=" +
						load.Succeeded + " lastError=" + load.LastError + " nodes=" +
						Names(client) + " owners=" + TestL2Topology.FmtMap(Owners(masters)));
					Assert.IsTrue(gone, victimName + " still in the node list or partition map, or a partition unowned, " +
						RoutingWaitMs + "ms after the scale down: nodes=" + Names(client));

					HashSet<string> remaining = PodNames(cluster.Pods());

					foreach (Node node in client.Nodes)
					{
						Assert.IsTrue(node.Active, node.Name + " is not active");
						Pod pod = PodOf(cluster.Pods(), node);
						Assert.IsNotNull(pod, node.Name + " is no remaining pod");
						Assert.IsTrue(remaining.Contains(pod.Name), pod.Name + " not in " + remaining);
					}
					Assert.IsFalse(Owners(masters).ContainsKey(victimName));

					long errors = Errors(client);
					long retries = client.Cluster.GetRetryCount();
					long failed = load.Failed;
					long succeeded = load.Succeeded;
					long window = QuietTendIntervals * policy.tendInterval;
					bool grew = K8sCluster.WaitFor(() => Errors(client) != errors ||
						client.Cluster.GetRetryCount() != retries || load.Failed != failed, window, 50);
					Console.WriteLine("L3-SCALE-DOWN-QUIET windowMs=" + window + " errors=" + errors + "->" + Errors(client) +
						" retries=" + retries + "->" + client.Cluster.GetRetryCount() + " failed=" + failed + "->" +
						load.Failed + " succeeded=" + succeeded + "->" + load.Succeeded + " lastError=" + load.LastError);
					Assert.IsFalse(grew, "Errors, retries or failed commands grew after " + victimName + " left");
					Assert.IsTrue(load.Succeeded > succeeded, "No command succeeded after " + victimName + " left");
				}
				AssertServedByEveryNode("scale-down", client);
			}
		}

		[TestMethod]
		public void NodePortsStableAcrossPodRescheduleAndRecreate()
		{
			if (foundSize < MinPods)
			{
				cluster.Scale(MinPods);
			}

			int size = cluster.Size();
			int port = cluster.ServicePort();
			List<Pod> pods = cluster.Pods();
			Pod first = pods[0];

			foreach (Pod pod in pods)
			{
				if (K8sCluster.Ordinal(pod.Name) < K8sCluster.Ordinal(first.Name))
				{
					first = pod;
				}
			}
			Assert.AreEqual(0, K8sCluster.Ordinal(first.Name), first.Name + " ordinal");

			int[] range = cluster.NodePortRange();
			HashSet<int> allocated = new HashSet<int>(K8sCluster.AllocatedNodePorts());
			int pinned = 0;

			for (int p = range[1]; p >= range[0] && pinned == 0; p--)
			{
				if (!allocated.Contains(p))
				{
					pinned = p;
				}
			}
			Assert.IsTrue(pinned > 0, "No free NodePort in " + range[0] + '-' + range[1]);

			string service = cluster.Name + "-pinned-seed";
			pinnedService = service;
			cluster.CreatePodNodePortService(service, first.Name, port, pinned);
			Console.WriteLine("L3-PINNED service=" + service + " pod=" + first.Name + " nodePort=" + pinned +
				" range=" + range[0] + '-' + range[1] + " allocated=" + allocated.Count);
			Assert.AreEqual(pinned, cluster.NodePort(service, port));
			AssertPinnedSeed("created", service, port, pinned, size);

			string podUid = cluster.Uid("pod", first.Name);
			string serviceUid = cluster.Uid("service", first.Name);
			int nodePort = cluster.NodePort(first.Name, port);
			cluster.DeletePod(first.Name);
			string name = first.Name;
			bool rescheduled = K8sCluster.WaitFor(() =>
			{
				string uid = cluster.Uid("pod", name);

				if (uid.Length == 0 || uid == podUid)
				{
					return false;
				}

				foreach (Pod pod in cluster.Pods())
				{
					if (pod.Name == name && pod.Phase == "Running")
					{
						return true;
					}
				}
				return false;
			}, K8sCluster.ScaleTimeoutMs, 2000);
			Assert.IsTrue(rescheduled, name + " not Running again within " + K8sCluster.ScaleTimeoutMs + "ms");
			cluster.Scale(size);
			Console.WriteLine("L3-POD-RESCHEDULED pod=" + name + " podUid=" + podUid + "->" + cluster.Uid("pod", name) +
				" serviceUid=" + serviceUid + "->" + cluster.Uid("service", name) + " nodePort=" + nodePort + "->" +
				cluster.NodePort(name, port));
			Assert.AreEqual(serviceUid, cluster.Uid("service", name), name + " Service UID");
			Assert.AreEqual(nodePort, cluster.NodePort(name, port), name + " Service NodePort");
			Assert.AreEqual(pinned, cluster.NodePort(service, port));
			AssertPinnedSeed("rescheduled", service, port, pinned, size);

			Pod last = HighestOrdinal(cluster.Pods());
			string lastUid = cluster.Uid("service", last.Name);
			int lastNodePort = cluster.NodePort(last.Name, port);
			cluster.Scale(size - 1);
			string lastName = last.Name;
			Assert.IsTrue(K8sCluster.WaitFor(() => cluster.Uid("service", lastName).Length == 0,
				K8sCluster.ScaleTimeoutMs, 1000), lastName + " Service not deleted within " + K8sCluster.ScaleTimeoutMs + "ms");

			// Hold the old NodePort so the allocator cannot hand it back to the recreated Service by chance.
			string held = cluster.Name + "-held-node-port";
			heldService = held;
			cluster.CreatePodNodePortService(held, last.Name, port, lastNodePort);
			cluster.Scale(size);
			string recreatedUid = cluster.Uid("service", last.Name);
			int recreatedNodePort = cluster.NodePort(last.Name, port);
			cluster.DeleteService(held);
			heldService = null;
			Console.WriteLine("L3-POD-RECREATED pod=" + last.Name + " serviceUid=" + lastUid + "->(deleted)->" +
				recreatedUid + " oldNodePort=" + lastNodePort + " newNodePort=" + recreatedNodePort);
			Assert.IsFalse(string.IsNullOrEmpty(recreatedUid), last.Name + " has no Service after it was recreated");
			Assert.AreNotEqual(lastUid, recreatedUid, last.Name + " Service not recreated");
			Assert.IsTrue(recreatedNodePort > 0, last.Name + " has no Service NodePort for " + port);
			Assert.AreNotEqual(lastNodePort, recreatedNodePort, last.Name + " recreated Service kept NodePort " + lastNodePort);
			Assert.AreEqual(pinned, cluster.NodePort(service, port));
			AssertPinnedSeed("recreated", service, port, pinned, size);
		}

		private void AssertPinnedSeed(string step, string service, int port, int pinned, int size)
		{
			Host seed = new Host(K8sCluster.EndpointHostname, pinned);

			Assert.IsTrue(K8sCluster.WaitFor(() => cluster.ReadyEndpoints(service) > 0, RoutingWaitMs, 500),
				service + " has no ready endpoint within " + RoutingWaitMs + "ms");

			using (AerospikeClient client = Connect(seed))
			{
				AwaitRouting("pinned-" + step, client, size);
				List<Pod> pods = cluster.Pods();

				foreach (Node node in client.Nodes)
				{
					Pod pod = PodOf(pods, node);
					Assert.IsNotNull(pod, node.Name + " is no pod in " + FormatPods(pods));
					string advertised = Info.Request(null, node, "service-clear-alt");
					int nodePort = cluster.NodePort(pod.Name, port);
					Host h = node.Host;
					Console.WriteLine("L3-PINNED-NODE step=" + step + " node=" + node.Name + " pod=" + pod.Name +
						" alternate=" + advertised + " serviceNodePort=" + nodePort + " pinned=" + pinned + " clientHost=" +
						h.name + ':' + h.port);
					Assert.AreEqual(K8sCluster.EndpointHostname + ':' + nodePort, advertised,
						node.Name + " (" + pod.Name + ") advertised alternate address");
					Assert.AreNotEqual(pinned, nodePort, pod.Name + " advertises the pinned NodePort of " + service);
				}
				AssertServedByEveryNode("pinned-" + step, client);
			}
		}

		private static AerospikeClient Connect(Host seed)
		{
			AerospikeClient client = null;
			AerospikeException error = null;
			bool ok = K8sCluster.WaitFor(() =>
			{
				try
				{
					if (client != null)
					{
						client.Dispose();
						client = null;
					}
					client = new AerospikeClient(Policy(), seed);
					return true;
				}
				catch (AerospikeException.Connection e)
				{
					error = e;
					return false;
				}
			}, RoutingWaitMs, 1000);

			if (!ok)
			{
				if (client != null)
				{
					client.Dispose();
				}
				throw error ?? new AerospikeException("Seed not reachable: " + seed);
			}
			return client;
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
			CaptureLog();
			return policy;
		}

		private static void AwaitRouting(string step, AerospikeClient client, int size)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			bool routed = K8sCluster.WaitFor(() => client.Nodes.Length == size && Unowned(client, masters) == 0 &&
				Owners(masters).Count == size, RoutingWaitMs, 50);
			Console.WriteLine("L3-ROUTING step=" + step + " nodes=" + Names(client) + " unowned=" +
				Unowned(client, masters) + " owners=" + TestL2Topology.FmtMap(Owners(masters)));
			Assert.IsTrue(routed, "Client did not hold " + size + " nodes owning every partition within " +
				RoutingWaitMs + "ms: " + Names(client));
		}

		private static void AssertServedByEveryNode(string step, AerospikeClient client)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			Assert.AreEqual(0, Unowned(client, masters));
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			wp.replica = Replica.MASTER;
			Policy rp = new Policy(client.ReadPolicyDefault);
			rp.replica = Replica.MASTER;

			foreach (Node node in client.Nodes)
			{
				Key key = null;

				for (int i = 0; i < KeySearchLimit && key == null; i++)
				{
					Key candidate = new Key(ns, Set, step + '-' + i);

					if (ReferenceEquals(masters[(int)Partition.GetPartitionId(candidate.digest)], node))
					{
						key = candidate;
					}
				}
				Assert.IsNotNull(key, node.Name + " is master of no partition");
				Assert.AreSame(node, Partition.Write(client.Cluster, wp, key).GetNodeWrite(client.Cluster));
				client.Put(wp, key, new Bin("v", node.Name));
				Assert.AreEqual(node.Name, client.Get(rp, key).GetString("v"));
				Console.WriteLine("L3-SERVED step=" + step + " node=" + node.Name + " key=" + key.userKey +
					" partition=" + Partition.GetPartitionId(key.digest) + " write=ok read=ok");
			}
		}

		private sealed class Load : IDisposable
		{
			public long Succeeded;
			public long Failed;
			public string LastError;
			private volatile bool stop;
			private readonly Thread[] threads;

			public Load(AerospikeClient client)
			{
				threads = new Thread[LoadThreads];

				for (int t = 0; t < LoadThreads; t++)
				{
					int thread = t;
					threads[t] = new Thread(() =>
					{
						for (int i = 0; !stop; i++)
						{
							Key key = new Key(ns, Set, "load-" + ((thread + i * LoadThreads) % LoadKeys));

							try
							{
								if (i % 2 == 0)
								{
									client.Put(null, key, new Bin("v", i));
								}
								else
								{
									client.Get(null, key);
								}
								Interlocked.Increment(ref Succeeded);
							}
							catch (Exception e)
							{
								Interlocked.Increment(ref Failed);
								LastError = e.GetType().Name + ": " + e.Message;
							}
						}
					})
					{
						IsBackground = true
					};
					threads[t].Start();
				}
			}

			public void Dispose()
			{
				stop = true;

				foreach (Thread thread in threads)
				{
					Assert.IsTrue(thread.Join(TimeSpan.FromMilliseconds(RoutingWaitMs)), "Load thread did not stop");
				}
			}
		}

		private static long Errors(AerospikeClient client)
		{
			long errors = 0;

			foreach (Node node in client.Nodes)
			{
				errors += node.GetErrorCount() + node.GetTimeoutCount();
			}
			return errors;
		}

		private static bool InPartitionMap(AerospikeClient client, string name)
		{
			Partitions partitions = client.Cluster.partitionMap[ns];

			if (partitions == null)
			{
				return false;
			}

			foreach (Node[] replica in partitions.replicas)
			{
				for (int i = 0; i < replica.Length; i++)
				{
					Node node = replica[i];

					if (node != null && node.Name == name)
					{
						return true;
					}
				}
			}
			return false;
		}

		private static int Unowned(AerospikeClient client, Node[] masters)
		{
			Partitions partitions = client.Cluster.partitionMap[ns];
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = partitions == null ? null : partitions.replicas[0][i];

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

		private static List<string> Names(AerospikeClient client)
		{
			List<string> names = new List<string>();

			foreach (Node node in client.Nodes)
			{
				names.Add(node.Name);
			}
			return names;
		}

		private static HashSet<string> PodNames(List<Pod> pods)
		{
			HashSet<string> names = new HashSet<string>();

			foreach (Pod pod in pods)
			{
				names.Add(pod.Name);
			}
			return names;
		}

		private static Pod HighestOrdinal(List<Pod> pods)
		{
			Pod last = pods[0];

			foreach (Pod pod in pods)
			{
				if (K8sCluster.Ordinal(pod.Name) > K8sCluster.Ordinal(last.Name))
				{
					last = pod;
				}
			}
			return last;
		}

		private static Pod PodOf(List<Pod> pods, Node node)
		{
			string access = Info.Request(null, node, "service-clear-std");

			foreach (Pod pod in pods)
			{
				if (HasAddress(access, pod.Ip))
				{
					return pod;
				}
			}
			return null;
		}

		private static Node NodeOf(AerospikeClient client, Pod pod)
		{
			foreach (Node node in client.Nodes)
			{
				if (HasAddress(Info.Request(null, node, "service-clear-std"), pod.Ip))
				{
					return node;
				}
			}
			return null;
		}

		private static bool HasAddress(string access, string ip)
		{
			if (access == null)
			{
				return false;
			}

			foreach (Host h in Host.ParseServiceHosts(access))
			{
				if (h.name == ip)
				{
					return true;
				}
			}
			return false;
		}

		private static long PeersGeneration(Node node)
		{
			return long.Parse(Info.Request(null, node, "peers-generation"));
		}

		private static void CaptureLog()
		{
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) => Console.WriteLine("CLIENT-LOG " + level + ' ' + message));
		}

		private static string Get(Dictionary<string, string> info, string key)
		{
			return info.TryGetValue(key, out string value) ? value : null;
		}

		private static string FormatPods(List<Pod> pods)
		{
			List<string> parts = new List<string>();

			foreach (Pod pod in pods)
			{
				parts.Add(pod.ToString());
			}
			return TestL2Topology.Fmt(parts);
		}
	}
}
