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
using Load = Aerospike.Test.L5Client.Load;

namespace Aerospike.Test
{
	/// <summary>
	/// One pass-through client under a steady read and write load through a real Spot interruption of
	/// the instance under one pod, then a rollout that replaces every Aerospike node. After each, the
	/// cluster recovers, every replaced pod advertises the endpoint name and its own NodePort, and the
	/// client holds every node, owns every partition and stops accumulating errors and retries.
	/// </summary>
	[TestClass]
	public class TestL5SpotInterruptAndRollout
	{
		private const string Set = "l5-replace";
		private const long SpotWaitMs = 25 * 60000L;
		private const long RolloutWaitMs = 95 * 60000L;
		private const long PodReplacedMs = 20 * 60000L;
		private const long ClientRecoveryMs = 60000;
		private const long QuietWaitMs = 60000;

		private L5Environment env;
		private K8sCluster cluster;
		private int foundSize;

		[TestInitialize]
		public void RequireEnvironment()
		{
			env = L5Environment.Require();
			env.WaitReady();
			env.WaitHealthy();
			cluster = K8sCluster.Find();
			foundSize = cluster.Size();
			Assert.AreEqual(L5Environment.ClusterSize, foundSize, "AerospikeCluster size");
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (cluster != null && env != null)
			{
				env.RestoreSize(cluster, foundSize);
			}
		}

		[TestMethod]
		public void SpotInterruptionAndRolloutRecoverUnderLoad()
		{
			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			L5Client.CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, env.Seed()))
			{
				L5Client.AwaitRouting("start", client, foundSize, L5Client.RoutingWaitMs);
				L5Client.AssertAdvertisement("start", env, client, L5Client.PodEndpoints(cluster));

				using (Load load = new Load(client, Set))
				{
					Pod victim = cluster.Pods()[0];
					SortedDictionary<string, string[]> before = Placements();
					long failed = load.Failed;
					Stopwatch start = Stopwatch.StartNew();
					env.Ctl(SpotWaitMs, "spot-interrupt", victim.Name, "--wait").AssertSucceeded();
					string[] old = before[victim.Name];
					bool replaced = K8sCluster.WaitFor(() =>
					{
						string[] now = cluster.PodPlacement(victim.Name);
						return now[0].Length > 0 && now[0] != old[0] && now[1].Length > 0 && now[1] != old[1];
					}, PodReplacedMs, 5000);
					Assert.IsTrue(replaced, victim.Name + " not rescheduled off " + old[1] + " within " +
						PodReplacedMs + "ms");
					Recover("spot-interrupt", client, load, before, new List<string> { victim.Name }, start, failed);

					before = Placements();
					failed = load.Failed;
					start.Restart();
					env.Ctl(RolloutWaitMs, "rollout", "all", "--wait").AssertSucceeded();
					Recover("rollout", client, load, before, before.Keys.ToList(), start, failed);
				}
			}
		}

		/// <summary>
		/// Wait for the cluster to be ready with every listener healthy, log each pod's old and new
		/// instance, and assert that each pod in <paramref name="replaced"/> runs on a new instance,
		/// every pod advertises the endpoint name with its own NodePort, the client holds every node
		/// with every partition owned, and its error and retry counts stop growing.
		/// </summary>
		private void Recover(string step, AerospikeClient client, Load load,
			SortedDictionary<string, string[]> before, List<string> replaced, Stopwatch start, long failedBefore)
		{
			env.WaitReady();
			env.WaitHealthy();
			long readyMs = start.ElapsedMilliseconds;
			SortedDictionary<string, string[]> after = Placements();

			foreach (KeyValuePair<string, string[]> entry in before)
			{
				after.TryGetValue(entry.Key, out string[] now);
				Console.WriteLine("L5-REPLACED step=" + step + " pod=" + entry.Key + " oldNode=" + entry.Value[1] +
					" oldInstance=" + entry.Value[2] + " newNode=" + (now == null ? null : now[1]) +
					" newInstance=" + (now == null ? null : now[2]));
			}

			CollectionAssert.AreEqual(before.Keys.ToList(), after.Keys.ToList(), step + ": pods");

			foreach (string pod in replaced)
			{
				Assert.AreNotEqual(before[pod][2], after[pod][2], step + ": " + pod + " instance");
			}

			L5Client.AwaitRouting(step, client, foundSize, ClientRecoveryMs);
			SortedDictionary<string, L5Client.PodEndpoint> endpoints = L5Client.PodEndpoints(cluster);
			L5Client.AssertAdvertisement(step, env, client, endpoints);
			L5Client.AssertNodesAtEndpoint(step, env, client, endpoints);
			long routedMs = start.ElapsedMilliseconds;
			L5Client.AwaitQuiet(step, client, load, QuietWaitMs);
			Console.WriteLine("L5-RECOVERY step=" + step + " clusterReadyMs=" + readyMs + " clientRoutedMs=" +
				routedMs + " quietMs=" + start.ElapsedMilliseconds + " failedCommands=" +
				(load.Failed - failedBefore) + " succeededCommands=" + load.Succeeded + " retries=" +
				client.Cluster.GetRetryCount() + " clientRecoveryBoundMs=" + ClientRecoveryMs +
				" quietBoundMs=" + QuietWaitMs + " lastError=" + load.LastError);
		}

		/// <summary>
		/// Return each pod's UID, node and instance.
		/// </summary>
		private SortedDictionary<string, string[]> Placements()
		{
			SortedDictionary<string, string[]> map =
				new SortedDictionary<string, string[]>(StringComparer.Ordinal);

			foreach (Pod pod in cluster.Pods())
			{
				map[pod.Name] = new[] { cluster.Uid("pod", pod.Name), pod.Node, K8sCluster.Instance(pod.Node) };
			}

			return map;
		}
	}
}
