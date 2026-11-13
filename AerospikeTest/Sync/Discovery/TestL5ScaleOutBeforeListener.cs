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
using System.Globalization;
using System.Net.Sockets;
using Aerospike.Client;
using Pod = Aerospike.Test.K8sCluster.Pod;
using Load = Aerospike.Test.L5Client.Load;
using Line = Aerospike.Test.L5Environment.Line;
using Result = Aerospike.Test.L5Environment.Result;

namespace Aerospike.Test
{
	/// <summary>
	/// A pod added while its NodePort has no listener: first Pending for want of an Aerospike node,
	/// then running and advertised by the server at a NodePort the endpoint does not serve yet, then
	/// served once the listener is added. One pass-through client under a steady load keeps serving
	/// through all three and uses the new node within 2 x tend interval of its listener serving.
	/// </summary>
	[TestClass]
	public class TestL5ScaleOutBeforeListener
	{
		private const string Set = "l5-scale-out";
		private const long PendingWaitMs = 300000;
		private const long AddNodeWaitMs = 35 * 60000L;
		private const long SetNodesWaitMs = 35 * 60000L;
		private const long AdvertisedWaitMs = 15 * 60000L;
		private const long ListenerWaitMs = 20 * 60000L;
		private const long SyncListenersWaitMs = 20 * 60000L;
		private const long QuietWaitMs = 60000;
		private const int ProbeTimeoutMs = 50;

		private L5Environment env;
		private K8sCluster cluster;
		private int foundSize;
		private bool scaled;
		private string nodeGroupZone;
		private int nodeGroupSize = -1;

		[TestInitialize]
		public void RequireEnvironment()
		{
			env = L5Environment.Require("L5_NODEGROUPS");
			env.WaitReady();
			env.WaitHealthy();
			cluster = K8sCluster.Find();
			foundSize = cluster.Size();
			Assert.AreEqual(L5Environment.ClusterSize, foundSize, "AerospikeCluster size");
		}

		/// <summary>
		/// Scale back, set the node group back to its starting size and remove the added listener,
		/// whether or not the test passed.
		/// </summary>
		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (cluster == null || env == null)
			{
				return;
			}

			try
			{
				if (scaled)
				{
					env.Ctl(L5Environment.CtlTimeoutMs, "scale", foundSize.ToString()).AssertSucceeded();
					env.WaitReady();
				}
			}
			finally
			{
				try
				{
					if (nodeGroupZone != null && nodeGroupSize >= 0)
					{
						env.Ctl(SetNodesWaitMs, "set-nodes", nodeGroupZone, nodeGroupSize.ToString(), "--wait")
							.AssertSucceeded();
						env.WaitReady();
					}
				}
				finally
				{
					if (scaled)
					{
						env.Ctl(SyncListenersWaitMs, "sync-listeners", "--prune").AssertSucceeded();
					}

					env.RestoreSize(cluster, foundSize);
				}
			}
		}

		[TestMethod]
		public void PodAdvertisedBeforeItsListenerJoinsOnceServed()
		{
			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			string watched = "";
			int warnings = 0;
			string lastWarning = null;
			L5Client.CaptureLog(message =>
			{
				string token = Volatile.Read(ref watched);

				if (!string.IsNullOrEmpty(token) && message.Contains(token, StringComparison.Ordinal))
				{
					Interlocked.Increment(ref warnings);
					Volatile.Write(ref lastWarning, message);
				}
			});
			int port = cluster.ServicePort();
			List<string> existingPods = cluster.Pods().Select(p => p.Name).ToList();

			using (AerospikeClient client = new AerospikeClient(policy, env.Seed()))
			{
				L5Client.AwaitRouting("start", client, foundSize, L5Client.RoutingWaitMs);
				List<string> existing = L5Client.Names(client);

				using (Load load = new Load(client, Set))
				{
					long failed = load.Failed;
					scaled = true;
					env.Ctl(L5Environment.CtlTimeoutMs, "scale", (foundSize + 1).ToString()).AssertSucceeded();
					Line pending = null;
					bool seen = K8sCluster.WaitFor(() =>
					{
						List<Line> lines = env.Ctl(L5Environment.CtlTimeoutMs, "pending").AssertSucceeded()
							.Tagged("L5-PENDING");
						pending = lines.Count == 0 ? null : lines[0];
						return pending != null;
					}, PendingWaitMs, 5000);
					Assert.IsTrue(seen, "No Pending pod within " + PendingWaitMs + "ms of scaling to " +
						(foundSize + 1));
					string pod = pending.Get("pod");
					string zone = pending.Get("zone");
					Assert.IsFalse(existingPods.Contains(pod), pod + " is an existing pod");
					Assert.AreNotEqual("unknown", zone, pod + " rack has no zone");
					Assert.AreEqual("Pending", Phase(pod), pod + " phase");
					L5Client.AwaitQuiet("pending", client, load, QuietWaitMs);
					Console.WriteLine("L5-PENDING-TRAFFIC pod=" + pod + " zone=" + zone + " nodes=" +
						string.Join(",", L5Client.Names(client)) + " failedSinceScale=" +
						(load.Failed - failed) + " lastError=" + load.LastError);
					Assert.AreEqual(failed, load.Failed, "Commands failed while " + pod + " was Pending");
					CollectionAssert.AreEqual(existing, L5Client.Names(client),
						"Client nodes while " + pod + " was Pending");

					Result added = env.Ctl(AddNodeWaitMs, "add-node", zone, "--wait");
					List<Line> nodes = added.Tagged("L5-NODES");

					if (nodes.Count > 0)
					{
						nodeGroupZone = zone;
						nodeGroupSize = int.Parse(nodes[0].Get("desired")) - 1;
					}

					added.AssertSucceeded();
					added.First("L5-NODES");

					int nodePort = 0;
					string name = "";
					bool advertised = K8sCluster.WaitFor(() =>
					{
						if (!"Running".Equals(Phase(pod), StringComparison.Ordinal))
						{
							return false;
						}

						nodePort = cluster.NodePort(pod, port);
						string id = cluster.NodeId(pod);

						if (nodePort == 0 || !System.Text.RegularExpressions.Regex.IsMatch(id, "^[0-9A-Fa-f]{1,16}$"))
						{
							return false;
						}

						name = Convert.ToUInt64(id, 16).ToString("X", CultureInfo.InvariantCulture);
						return AdvertisedByEveryNode(client, name, nodePort);
					}, AdvertisedWaitMs, 2000);
					Assert.IsTrue(advertised, pod + " not running and advertised at " + env.EndpointName +
						" with its NodePort by every node within " + AdvertisedWaitMs + "ms");
					Volatile.Write(ref watched, ":" + nodePort);
					bool early = Serving(nodePort);
					Console.WriteLine("L5-ADVERTISED-BEFORE-LISTENER pod=" + pod + " node=" + name +
						" advertised=" + env.Advertised(nodePort) + " serving=" + early);
					Assert.IsFalse(early, env.Advertised(nodePort) + " already serves before its listener was added");

					long windowMs = (long)L5Client.QuietTendIntervals * L5Environment.TendInterval;
					long windowFailed = load.Failed;
					long windowSucceeded = load.Succeeded;
					Stopwatch window = Stopwatch.StartNew();
					List<string> stuck = new List<string>();
					bool held = false;
					K8sCluster.WaitFor(() =>
					{
						foreach (string node in L5Client.Unreachable(client))
						{
							if (!stuck.Contains(node))
							{
								stuck.Add(node);
							}
						}

						held |= L5Client.NodeNamed(client, name) != null;
						return window.ElapsedMilliseconds >= windowMs;
					}, windowMs + L5Client.RoutingWaitMs, 50);
					long observedMs = window.ElapsedMilliseconds;
					Console.WriteLine("L5-UNREACHABLE-PEER pod=" + pod + " node=" + name + " advertised=" +
						env.Advertised(nodePort) + " observedMs=" + observedMs + " windowMs=" + windowMs +
						" clientNodes=" + string.Join(",", L5Client.Names(client)) + " heldNewNode=" + held +
						" stuckNodes=" + string.Join(",", stuck) + " warnings=" + warnings + " lastWarning=" +
						lastWarning + " failed=" + windowFailed + "->" + load.Failed + " succeeded=" +
						windowSucceeded + "->" + load.Succeeded + " lastError=" + load.LastError);
					Assert.IsTrue(observedMs >= windowMs, "Window observed " + observedMs + "ms, under " +
						windowMs + "ms");
					Assert.IsTrue(stuck.Count == 0, "Client holds nodes it cannot reach: " +
						string.Join(",", stuck));
					Assert.AreEqual(windowFailed, load.Failed,
						"Commands failed while " + pod + " had no listener");
					Assert.IsTrue(load.Succeeded > windowSucceeded,
						"No command succeeded while " + pod + " had no listener");

					Task<long[]> listener = Task.Run(() =>
					{
						env.Ctl(SyncListenersWaitMs, "sync-listeners").AssertSucceeded();
						long synced = Stopwatch.GetTimestamp();
						env.WaitHealthy();
						return new[] { synced, Stopwatch.GetTimestamp() };
					});
					long servingAt = 0;
					long lastClosedAt = 0;
					long usableAt = 0;
					Node[] masters = new Node[Node.PARTITIONS];
					K8sCluster.WaitFor(() =>
					{
						long probeAt = Stopwatch.GetTimestamp();

						if (servingAt == 0)
						{
							if (Serving(nodePort))
							{
								servingAt = probeAt;
							}
							else
							{
								lastClosedAt = probeAt;
							}
						}

						if (servingAt != 0 && usableAt == 0)
						{
							Node node = L5Client.NodeNamed(client, name);

							if (node != null && node.Active && client.Nodes.Length == foundSize + 1 &&
								L5Client.Unowned(client, masters, false) == 0)
							{
								usableAt = Stopwatch.GetTimestamp();
							}
						}

						return usableAt != 0 || listener.IsFaulted ||
							(servingAt != 0 && TicksToMs(Stopwatch.GetTimestamp() - servingAt) >
								L5Client.RoutingWaitMs);
					}, ListenerWaitMs, 10);
					long[] listenerTimes = listener.Wait(TimeSpan.FromMilliseconds(ListenerWaitMs))
						? listener.Result
						: throw new TimeoutException("sync-listeners did not finish");
					long budget = 2L * L5Environment.TendInterval;
					long usableMs = usableAt == 0 ? -1 : TicksToMs(usableAt - servingAt);
					Console.WriteLine("L5-NODE-USABLE pod=" + pod + " node=" + name + " advertised=" +
						env.Advertised(nodePort) + " listenerAddedAt=" + Instant(listenerTimes[0]) +
						" servingAt=" + (servingAt == 0 ? null : Instant(servingAt)) + " healthyReturnedAt=" +
						Instant(listenerTimes[1]) + " lastClosedProbeAt=" +
						(lastClosedAt == 0 ? null : Instant(lastClosedAt)) + " probeTimeoutMs=" +
						ProbeTimeoutMs + " usableAt=" + (usableAt == 0 ? null : Instant(usableAt)) +
						" servingToUsableMs=" + usableMs + " tendInterval=" + L5Environment.TendInterval +
						" budgetMs=" + budget + " nodes=" + string.Join(",", L5Client.Names(client)) +
						" owners=" + FormatOwners(L5Client.Owners(masters)));
					Assert.AreNotEqual(0, servingAt, "Listener for " + env.Advertised(nodePort) + " never served");
					Assert.AreNotEqual(0, usableAt, name + " not active with every partition owned once its " +
						"listener served");
					Assert.IsTrue(usableMs <= budget, name + " usable " + usableMs +
						"ms after its listener served, budget " + budget + "ms");

					L5Client.AwaitRouting("served", client, foundSize + 1, L5Client.RoutingWaitMs);
					L5Client.AssertEveryPartitionServed("served", client, Set);
				}
			}
		}

		private string Phase(string pod)
		{
			foreach (Pod p in cluster.Pods())
			{
				if (p.Name.Equals(pod, StringComparison.Ordinal))
				{
					return p.Phase;
				}
			}

			return "";
		}

		/// <summary>
		/// Return whether every node the client holds lists <paramref name="name"/> in its
		/// peers-clear-alt reply at the endpoint name and <paramref name="nodePort"/> alone.
		/// </summary>
		private bool AdvertisedByEveryNode(AerospikeClient client, string name, int nodePort)
		{
			foreach (Node node in client.Nodes)
			{
				List<string> addresses;

				try
				{
					L5Client.Peers(Info.Request(null, node, "peers-clear-alt")).TryGetValue(name, out addresses);
				}
				catch (AerospikeException)
				{
					return false;
				}

				if (addresses == null || addresses.Count != 1 ||
					!env.Advertised(nodePort).Equals(addresses[0], StringComparison.Ordinal))
				{
					return false;
				}
			}

			return client.Nodes.Length > 0;
		}

		/// <summary>
		/// Return whether a TCP connection to the endpoint name at <paramref name="nodePort"/> opens
		/// within <see cref="ProbeTimeoutMs"/>. The listener is taken to serve from the start of the
		/// first probe that connects.
		/// </summary>
		private bool Serving(int nodePort)
		{
			try
			{
				using TcpClient tcp = new TcpClient();
				IAsyncResult result = tcp.BeginConnect(env.EndpointName, nodePort, null, null);
				bool ok = result.AsyncWaitHandle.WaitOne(ProbeTimeoutMs);

				if (!ok)
				{
					return false;
				}

				tcp.EndConnect(result);
				return true;
			}
			catch (Exception)
			{
				return false;
			}
		}

		private static string Instant(long ticks)
		{
			long elapsed = Stopwatch.GetTimestamp() - ticks;
			return DateTimeOffset.UtcNow.AddSeconds(-(double)elapsed / Stopwatch.Frequency).ToString("O");
		}

		private static long TicksToMs(long ticks) => ticks * 1000L / Stopwatch.Frequency;

		private static string FormatOwners(IReadOnlyDictionary<string, int> owners)
		{
			return '{' + string.Join(", ", owners.Select(e => e.Key + '=' + e.Value)) + '}';
		}
	}
}
