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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Zero-node re-seed uses the seed snapshot held at the start of each tend iteration.
	/// </summary>
	[DoNotParallelize]
	[TestClass]
	public class TestZeroNodeReseed
	{
		private static readonly Host DeadSeed = new Host("127.0.0.1", 1);
		private const string ClusterName = "ZeroNodeReseedTest";
		private const string SeedPrefix = "Seed 127.0.0.1 ";

		private readonly List<string> events = new List<string>();
		private readonly FieldInfo seedsField = typeof(Cluster).GetField("seeds",
			BindingFlags.Instance | BindingFlags.NonPublic);
		private readonly FieldInfo tendThreadField = typeof(Cluster).GetField("tendThread",
			BindingFlags.Instance | BindingFlags.NonPublic);
		private readonly FieldInfo managerField = typeof(Cluster).GetField("seedCandidateManager",
			BindingFlags.Instance | BindingFlags.NonPublic);

		[TestInitialize]
		public void SetUp()
		{
			Log.SetLevel(Log.Level.WARN);
			Log.SetContextCallback((context, level, message) =>
			{
				if (context.clusterName != ClusterName)
				{
					return;
				}

				if (level == Log.Level.WARN && message.StartsWith(SeedPrefix, StringComparison.Ordinal))
				{
					int portStart = SeedPrefix.Length;
					int portEnd = message.IndexOf(' ', portStart);
					string port = portEnd > portStart
						? message.Substring(portStart, portEnd - portStart)
						: message.Substring(portStart);
					Event("seed " + port);
				}
			});
		}

		[TestCleanup]
		public void TearDown()
		{
			Log.SetContextCallback(null);
			Log.SetLevel(Log.Level.INFO);
		}

		[TestMethod]
		public void ReseedUsesSnapshotHeldAtStartOfTend()
		{
			ZeroNodeSeedProvider provider = new ZeroNodeSeedProvider(true, true);
			provider.Result = call =>
			{
				Event("refresh " + call);
				return [new Host("127.0.0.1", 10 + call)];
			};

			AerospikeClient client = new AerospikeClient(Policy(provider, 250), DeadSeed);

			try
			{
				Await(() => provider.Calls >= 4);
				AssertTendThreadOnly(client.Cluster);
			}
			finally
			{
				client.Close();
			}

			List<string> expected =
			[
				"refresh 1",
				"seed 1", "seed 11",
				"seed 1", "seed 11", "refresh 2",
				"seed 1", "seed 12", "refresh 3",
				"seed 1", "seed 13", "refresh 4"
			];
			CollectionAssert.AreEqual(expected, Events().GetRange(0, expected.Count));
		}

		[TestMethod]
		public void ReseedDoesNotWaitForRefreshInterval()
		{
			ZeroNodeSeedProvider provider = new ZeroNodeSeedProvider(true, true, new Host("127.0.0.1", 11));

			long begin = Environment.TickCount64;
			AerospikeClient client = new AerospikeClient(Policy(provider, 60000), DeadSeed);

			try
			{
				Cluster cluster = client.Cluster;
				Host[] seeds = GetSeeds(cluster);
				Await(() => Events().Count >= 8);
				long elapsed = Environment.TickCount64 - begin;

				Assert.IsTrue(elapsed < 5000, "three re-seeds took " + elapsed + "ms");
				Assert.AreEqual(1, provider.Calls);
				Assert.AreSame(seeds, GetSeeds(cluster));
				Assert.AreEqual(0, cluster.Nodes.Length);
				AssertTendThreadOnly(cluster);
			}
			finally
			{
				client.Close();
			}

			List<string> attempts = Events();

			for (int i = 0; i < attempts.Count; i++)
			{
				Assert.AreEqual(i % 2 == 0 ? "seed 1" : "seed 11", attempts[i]);
			}
		}

		private void AssertTendThreadOnly(Cluster cluster)
		{
			Thread tend = (Thread)tendThreadField.GetValue(cluster);
			Assert.IsNotNull(tend);
			Assert.AreEqual("tend", tend.Name);
			Assert.IsTrue(tend.IsAlive);

			SeedCandidateManager manager = (SeedCandidateManager)managerField.GetValue(cluster);
			Assert.IsFalse(manager.HasRefreshThread,
				"TEND execution mode must not start aerospike-seed-refresh");
		}

		private void Event(string name)
		{
			lock (events)
			{
				events.Add(name);
			}
		}

		private List<string> Events()
		{
			lock (events)
			{
				return new List<string>(events);
			}
		}

		private Host[] GetSeeds(Cluster cluster)
		{
			Assert.IsNotNull(seedsField);
			return (Host[])seedsField.GetValue(cluster);
		}

		private static ClientPolicy Policy(ZeroNodeSeedProvider provider, int refreshIntervalMs)
		{
			return new ClientPolicy
			{
				clusterName = ClusterName,
				failIfNotConnected = false,
				timeout = 100,
				tendInterval = 250,
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				discoveryRefreshIntervalMs = refreshIntervalMs,
				discoveryTendRefreshDeadlineMs = 20,
				seedCandidateProvider = provider
			};
		}

		private static void Await(Func<bool> condition)
		{
			long deadline = Environment.TickCount64 + 10000;

			while (!condition())
			{
				Assert.IsTrue(Environment.TickCount64 < deadline, "condition not met in time");
				Thread.Sleep(5);
			}
		}

		internal sealed class ZeroNodeSeedProvider : ISeedCandidateProvider
		{
			internal int Calls;
			internal Func<int, List<Host>> Result;
			private readonly bool periodic;
			private readonly bool deadline;

			internal ZeroNodeSeedProvider(bool periodic, bool deadline, params Host[] hosts)
			{
				this.periodic = periodic;
				this.deadline = deadline;
				List<Host> list = new List<Host>(hosts);
				Result = _ => list;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				int call = ++Calls;
				return Result(call);
			}

			public bool NeedsPeriodicRefresh() => periodic;

			public bool SupportsDeadline() => deadline;
		}
	}
}
