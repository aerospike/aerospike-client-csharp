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
using System.Net.Sockets;
using Aerospike.Client;
using Load = Aerospike.Test.L5Client.Load;
using Result = Aerospike.Test.L5Environment.Result;

namespace Aerospike.Test
{
	/// <summary>
	/// Five minutes without any traffic in or out of the zone of the endpoint address the client holds
	/// its nodes on, which also takes out that zone's rack, with one pass-through client under a steady
	/// load. During the outage no partition goes without an answering owner for longer than
	/// <see cref="OwnershipGapMs"/> and every command succeeds within <see cref="CommandBoundMs"/> of
	/// its first attempt; after it the cluster and the client are back to every node with every
	/// partition owned.
	/// </summary>
	[TestClass]
	public class TestL5AzOutage
	{
		private const string Set = "l5-az-outage";
		private const string Duration = "PT5M";
		private const long DurationMs = 5 * 60000L;
		private const long OwnershipGapMs = 30000;
		private const long CommandBoundMs = OwnershipGapMs + 3L * L5Environment.Timeout;
		private const long RunningWaitMs = 120000;
		private const long OutageWaitMs = DurationMs + 10 * 60000L;
		private const long ExperimentWaitMs = 25 * 60000L;
		private const long StatusPollMs = 15000;
		private const long RecoveryMs = 120000;

		private L5Environment env;
		private K8sCluster cluster;
		private int foundSize;
		private string experiment;
		private bool ended;

		[TestInitialize]
		public void RequireEnvironment()
		{
			env = L5Environment.Require("L5_RUNNER_AZ");
			env.WaitReady();
			env.WaitHealthy();
			cluster = K8sCluster.Find();
			foundSize = cluster.Size();
			Assert.AreEqual(L5Environment.ClusterSize, foundSize, "AerospikeCluster size");
		}

		/// <summary>
		/// Wait for the outage to end and the cluster to be ready, whether or not the test passed.
		/// </summary>
		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			if (cluster == null || env == null)
			{
				return;
			}

			if (experiment != null && !ended)
			{
				env.Ctl(ExperimentWaitMs, "experiment", experiment, "--wait");
				env.WaitReady();
			}

			env.RestoreSize(cluster, foundSize);
		}

		[TestMethod]
		public void ZoneOutageKeepsEveryPartitionOnReachableNodes()
		{
			ClientPolicy policy = env.Policy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			L5Client.CaptureLog();

			using (AerospikeClient client = new AerospikeClient(policy, env.Seed()))
			{
				L5Client.AwaitRouting("start", client, foundSize, L5Client.RoutingWaitMs);
				Console.WriteLine("L5-RESOLVED when=before name=" + env.EndpointName + " addresses=" +
					Resolved(env.EndpointName) + " clientHosts=" + string.Join(",", Hosts(client)));

				string zone = OutageZone(client);
				List<string> rackZones = cluster.RackZones();
				Console.WriteLine("L5-OUTAGE-ZONE zone=" + zone + " runnerAz=" + env.RunnerAz +
					" rackZones=" + string.Join(",", rackZones));
				Assert.IsTrue(rackZones.Contains(zone),
					"Endpoint zone " + zone + " has no rack: " + string.Join(",", rackZones));

				using (Load load = new Load(client, Set))
				{
					long failedBefore = load.Failed;
					Result started = env.Ctl(L5Environment.CtlTimeoutMs, "az-outage", zone, "--duration", Duration)
						.AssertSucceeded();
					experiment = started.First("L5-FIS").Get("experiment");

					string status = "";
					bool running = K8sCluster.WaitFor(() =>
					{
						status = Status();
						return "running".Equals(status, StringComparison.Ordinal) ||
							"completed".Equals(status, StringComparison.Ordinal);
					}, RunningWaitMs, 5000);
					Assert.IsTrue(running, "Experiment " + experiment + " not running within " +
						RunningWaitMs + "ms: " + status);
					Assert.AreEqual("running", status,
						"Experiment " + experiment + " ended before it was observed running");
					Stopwatch runningFor = Stopwatch.StartNew();
					long failedAtRunning = load.Failed;
					load.ResetMaxLatencyMs();
					long unownedSince = 0;
					long longestUnownedMs = 0;
					long lastStatusAt = 0;
					int unownedStretches = 0;
					bool resolvedLogged = false;
					Node[] masters = new Node[Node.PARTITIONS];
					bool over = K8sCluster.WaitFor(() =>
					{
						long now = runningFor.ElapsedMilliseconds;
						int unowned = L5Client.Unowned(client, masters, true);

						if (unowned != 0)
						{
							if (unownedSince == 0)
							{
								unownedSince = now;
								unownedStretches++;
								Console.WriteLine("L5-OUTAGE-UNOWNED zone=" + zone + " afterRunningMs=" + now +
									" unowned=" + unowned + " nodes=" + string.Join(",", L5Client.Names(client)) +
									" unreachable=" + string.Join(",", L5Client.Unreachable(client)));
							}

							longestUnownedMs = Math.Max(longestUnownedMs, now - unownedSince);
						}
						else if (unownedSince != 0)
						{
							Console.WriteLine("L5-OUTAGE-OWNED zone=" + zone + " afterRunningMs=" + now +
								" unownedMs=" + (now - unownedSince) + " boundMs=" + OwnershipGapMs + " nodes=" +
								string.Join(",", L5Client.Names(client)) + " unreachable=" +
								string.Join(",", L5Client.Unreachable(client)) + " owners=" +
								FormatOwners(L5Client.Owners(masters)) + " failedCommands=" +
								(load.Failed - failedAtRunning));
							unownedSince = 0;
						}

						if (!resolvedLogged && now >= DurationMs / 2)
						{
							resolvedLogged = true;
							Console.WriteLine("L5-RESOLVED when=during name=" + env.EndpointName +
								" addresses=" + Resolved(env.EndpointName) + " clientHosts=" +
								string.Join(",", Hosts(client)));
						}

						if (now - lastStatusAt >= StatusPollMs)
						{
							lastStatusAt = now;
							status = Status();
							return !"running".Equals(status, StringComparison.Ordinal);
						}

						return false;
					}, OutageWaitMs, 250);
					long outageMs = runningFor.ElapsedMilliseconds;
					long maxCommandMs = load.MaxLatencyMs();
					Console.WriteLine("L5-OUTAGE zone=" + zone + " experiment=" + experiment + " status=" +
						status + " observedMs=" + outageMs + " durationMs=" + DurationMs +
						" unownedStretches=" + unownedStretches + " longestUnownedMs=" + longestUnownedMs +
						" ownershipGapBoundMs=" + OwnershipGapMs + " maxCommandMs=" + maxCommandMs +
						" commandBoundMs=" + CommandBoundMs + " failedCommands=" +
						(load.Failed - failedAtRunning) + " failedCommandsSinceLoadStart=" +
						(load.Failed - failedBefore) + " succeededCommands=" + load.Succeeded + " retries=" +
						client.Cluster.GetRetryCount() + " lastError=" + load.LastError);
					Assert.IsTrue(over, "Experiment " + experiment + " still running " + OutageWaitMs +
						"ms after it started");
					Assert.IsTrue(longestUnownedMs <= OwnershipGapMs,
						"Partitions not owned by an answering node for " + longestUnownedMs +
						"ms during the outage, bound " + OwnershipGapMs + "ms");
					Assert.IsTrue(maxCommandMs <= CommandBoundMs,
						"A command took " + maxCommandMs + "ms to succeed during the outage, bound " +
						CommandBoundMs + "ms");

					env.Ctl(ExperimentWaitMs, "experiment", experiment, "--wait").AssertSucceeded();
					ended = true;
					env.WaitReady();
					env.WaitHealthy();
					L5Client.AwaitRouting("after", client, foundSize, RecoveryMs);
					Console.WriteLine("L5-RESOLVED when=after name=" + env.EndpointName + " addresses=" +
						Resolved(env.EndpointName) + " clientHosts=" + string.Join(",", Hosts(client)));
					L5Client.AssertEveryPartitionServed("after", client, Set);
				}
			}
		}

		/// <summary>
		/// Return the zone of the endpoint address that most of the client's nodes are on.
		/// </summary>
		private string OutageZone(AerospikeClient client)
		{
			SortedDictionary<string, int> nodesByZone =
				new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Node node in client.Nodes)
			{
				string address = node.NodeAddress.Address.ToString();
				string zone = env.Ctl(L5Environment.CtlTimeoutMs, "endpoint-zone", address).AssertSucceeded()
					.First("L5-ENDPOINT-ZONE").Get("zone");
				Console.WriteLine("L5-CLIENT-ENDPOINT node=" + node.Name + " address=" + address +
					" zone=" + zone);
				nodesByZone[zone] = nodesByZone.GetValueOrDefault(zone, 0) + 1;
			}

			Assert.IsTrue(nodesByZone.Count > 0, "Client has no nodes");
			return nodesByZone.OrderByDescending(e => e.Value).First().Key;
		}

		private string Status()
		{
			return env.Ctl(L5Environment.CtlTimeoutMs, "experiment", experiment).AssertSucceeded()
				.First("L5-FIS").Get("status");
		}

		private static string Resolved(string name)
		{
			try
			{
				return '[' + string.Join(", ", L5Client.Addresses(name)) + ']';
			}
			catch (SocketException)
			{
				return "unresolved";
			}
		}

		private static List<string> Hosts(AerospikeClient client)
		{
			List<string> hosts = new List<string>();

			foreach (Node node in client.Nodes)
			{
				hosts.Add(node.Name + '=' + node.Host.name + ':' + node.Host.port);
			}

			return hosts;
		}

		private static string FormatOwners(IReadOnlyDictionary<string, int> owners)
		{
			return '{' + string.Join(", ", owners.Select(e => e.Key + '=' + e.Value)) + '}';
		}
	}
}
