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
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// U-5 / merge-policy suite: ordering, empty refresh, trip restore (M-8).
	/// </summary>
	[TestClass]
	public class TestSeedMergePolicy
	{
		private static readonly Host S1 = new Host("10.0.0.1", 3000);
		private static readonly Host S2 = new Host("10.0.0.2", 3000);
		private static readonly Host D1 = new Host("10.0.1.1", 3000);
		private static readonly Host D2 = new Host("10.0.1.2", 3000);
		private static readonly Host[] StaticSeeds = [S1, S2];
		private static readonly Host[] Discovered = [D1, new Host("10.0.0.2", "tls", 3000), D2, D1];

		[TestMethod]
		public void PolicyDefaultAndCopy()
		{
			ClientPolicy policy = new ClientPolicy();
			Assert.AreEqual(SeedMergePolicy.MERGE, policy.seedMergePolicy);

			policy.seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY;
			Assert.AreEqual(SeedMergePolicy.DISCOVERY_ONLY, new ClientPolicy(policy).seedMergePolicy);

			policy.seedMergePolicy = SeedMergePolicy.REPLACE;
			Assert.AreEqual(SeedMergePolicy.REPLACE, new ClientPolicy(policy).seedMergePolicy);
		}

		[TestMethod]
		public void MergeOrder()
		{
			SeedCandidateManager manager = Create(SeedMergePolicy.MERGE);

			AssertHosts([S1, S2, D1, D2], manager.Merge(Discovered, true));
			Assert.AreSame(S2, manager.Merge(Discovered, true)[1]);
			AssertHosts([S1, S2, D1, D2], manager.Merge(Discovered, false));
			AssertHosts(StaticSeeds, manager.Merge([], true));
			AssertHosts(StaticSeeds, manager.Merge([], false));
		}

		[TestMethod]
		public void ReplaceOrder()
		{
			SeedCandidateManager manager = Create(SeedMergePolicy.REPLACE);

			AssertHosts([S1, S2, D1, D2], manager.Merge(Discovered, true));
			Host[] replaced = manager.Merge(Discovered, false);
			AssertHosts([D1, new Host("10.0.0.2", "tls", 3000), D2], replaced);
			Assert.AreEqual("tls", replaced[1].tlsName);
			AssertHosts(StaticSeeds, manager.Merge([], true));
			Assert.AreEqual(0, manager.Merge([], false).Length);
		}

		[TestMethod]
		public void DiscoveryOnlyOrder()
		{
			SeedCandidateManager manager = Create(SeedMergePolicy.DISCOVERY_ONLY);
			Host[] expected = [D1, new Host("10.0.0.2", "tls", 3000), D2];

			AssertHosts(expected, manager.Merge(Discovered, true));
			AssertHosts(expected, manager.Merge(Discovered, false));
			AssertHosts(StaticSeeds, manager.Merge([], true));
			AssertHosts(StaticSeeds, manager.Merge([], false));
		}

		[TestMethod]
		public void StaticSeedsCopiedFromCaller()
		{
			Host[] hosts = [S1, S2];
			SeedCandidateManager manager = Create(SeedMergePolicy.MERGE, hosts);
			hosts[0] = D2;

			AssertHosts(StaticSeeds, manager.Merge([], false));
			Assert.AreEqual(D2, hosts[0]);
			Assert.AreEqual(S2, hosts[1]);
		}

		[TestMethod]
		public void MergeRefreshPublishesStaticThenDiscovered()
		{
			Host[] snapshot = null;
			TestProvider provider = new TestProvider(true) { Next = [D1] };
			SeedCandidateManager manager = Create(SeedMergePolicy.MERGE, StaticSeeds, provider, h => snapshot = h);

			manager.Start();
			AssertHosts([S1, S2, D1], snapshot);

			provider.Next = [D2];
			manager.Tend(1, 1000);
			AssertHosts([S1, S2, D2], snapshot);

			provider.Next = [];
			manager.Tend(2, 1000);
			AssertHosts(StaticSeeds, snapshot);

			manager.Close();
		}

		[TestMethod]
		public void ReplaceDropsStaticAfterInitAndKeepsPreviousOnEmpty()
		{
			Host[] snapshot = null;
			int publishes = 0;
			TestProvider provider = new TestProvider(true) { Next = [D1] };
			SeedCandidateManager manager = Create(SeedMergePolicy.REPLACE, StaticSeeds, provider, h =>
			{
				publishes++;
				snapshot = h;
			});

			manager.Start();
			AssertHosts([S1, S2, D1], snapshot);
			int afterInit = publishes;

			provider.Next = [D2, S1];
			manager.Tend(1, 1000);
			Host[] replaced = snapshot;
			AssertHosts([D2, S1], replaced);

			provider.Next = [];
			manager.Tend(2, 1000);
			Assert.AreSame(replaced, snapshot);
			Assert.AreEqual(afterInit + 1, publishes);

			manager.Close();
		}

		[TestMethod]
		public void DiscoveryOnlyFallsBackToStaticOnEmpty()
		{
			Host[] snapshot = null;
			TestProvider provider = new TestProvider(true) { Next = [] };
			SeedCandidateManager manager = Create(SeedMergePolicy.DISCOVERY_ONLY, StaticSeeds, provider, h => snapshot = h);

			manager.Start();
			AssertHosts(StaticSeeds, snapshot);

			provider.Next = [D1, D2];
			manager.Tend(1, 1000);
			AssertHosts([D1, D2], snapshot);

			provider.Next = [];
			manager.Tend(2, 1000);
			AssertHosts(StaticSeeds, snapshot);

			manager.Close();
		}

		[TestMethod]
		public void FailedOrEmptyRefreshNeverEmptiesSnapshot()
		{
			foreach (SeedMergePolicy merge in new[] { SeedMergePolicy.MERGE, SeedMergePolicy.REPLACE, SeedMergePolicy.DISCOVERY_ONLY })
			{
				Host[] snapshot = null;
				TestProvider provider = new TestProvider(true) { Next = [D1] };
				SeedCandidateManager manager = Create(merge, StaticSeeds, provider, h => snapshot = h);

				manager.Start();
				provider.Next = [D2];
				manager.Tend(1, 1000);
				Host[] previous = snapshot;
				Assert.IsTrue(previous.Length > 0, merge.ToString());

				provider.Error = new InvalidOperationException("fail");
				manager.Tend(2, 1000);
				Assert.AreSame(previous, snapshot, merge.ToString());

				provider.Error = null;
				provider.Next = null;
				manager.Tend(3, 1000);
				Assert.AreSame(previous, snapshot, merge.ToString());

				provider.Next = [];
				manager.Tend(4, 1000);
				Assert.IsTrue(snapshot.Length > 0, merge.ToString());

				manager.Close();
			}
		}

		[TestMethod]
		public void TripRestoresMergedInitSnapshot()
		{
			foreach (SeedMergePolicy merge in new[] { SeedMergePolicy.MERGE, SeedMergePolicy.REPLACE, SeedMergePolicy.DISCOVERY_ONLY })
			{
				Host[] snapshot = null;
				TestProvider provider = new TestProvider(true) { Next = [D1], SleepMs = 0 };
				SeedCandidateManager manager = Create(merge, StaticSeeds, provider, h => snapshot = h,
					tendDeadlineMs: 1);

				manager.Start();
				Host[] initSeeds = snapshot;

				provider.Next = [D2];
				provider.SleepMs = 0;
				manager.Tend(1, 1000);

				provider.SleepMs = 20;

				for (int i = 1; i <= 3; i++)
				{
					manager.Tend(1 + i, 1000);
				}

				Assert.IsTrue(manager.IsDeadlineCircuitOpen, merge.ToString());
				Assert.AreSame(initSeeds, snapshot, merge.ToString());
				manager.Close();
			}
		}

		[TestMethod]
		public void MergeRunsOnlyWhenCadenceHits()
		{
			Host[] snapshot = null;
			int publishes = 0;
			TestProvider provider = new TestProvider(true) { Next = [D1] };
			ClientPolicy policy = TendPolicy(SeedMergePolicy.MERGE, 10000, 20);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, StaticSeeds, h =>
				{
					publishes++;
					snapshot = h;
				}, Log.Context.Empty);

			manager.Start();
			Host[] initSeeds = snapshot;
			int afterInit = publishes;

			for (int tendCount = 1; tendCount < 10; tendCount++)
			{
				manager.Tend(tendCount, 1000);
				Assert.AreSame(initSeeds, snapshot);
			}

			Assert.AreEqual(afterInit, publishes);
			manager.Tend(10, 1000);
			Assert.AreEqual(afterInit + 1, publishes);
			AssertHosts([S1, S2, D1], snapshot);
			manager.Close();
		}

		private static SeedCandidateManager Create(SeedMergePolicy merge)
		{
			return Create(merge, StaticSeeds, new TestProvider(false), _ => { });
		}

		private static SeedCandidateManager Create(SeedMergePolicy merge, Host[] staticSeeds)
		{
			return Create(merge, staticSeeds, new TestProvider(false), _ => { });
		}

		private static SeedCandidateManager Create(
			SeedMergePolicy merge,
			Host[] staticSeeds,
			TestProvider provider,
			Action<Host[]> publisher,
			int tendDeadlineMs = 20)
		{
			ClientPolicy policy = TendPolicy(merge, 1000, tendDeadlineMs);
			return new SeedCandidateManager(provider, policy, staticSeeds, publisher, Log.Context.Empty);
		}

		private static ClientPolicy TendPolicy(SeedMergePolicy merge, int refreshIntervalMs, int tendDeadlineMs)
		{
			return new ClientPolicy
			{
				seedMergePolicy = merge,
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = refreshIntervalMs,
				discoveryTendRefreshDeadlineMs = tendDeadlineMs
			};
		}

		private static void AssertHosts(Host[] expected, Host[] actual)
		{
			Assert.AreEqual(expected.Length, actual.Length, string.Join(",", Array.ConvertAll(actual, h => h.ToString())));

			for (int i = 0; i < expected.Length; i++)
			{
				Assert.AreEqual(expected[i].name, actual[i].name, "index " + i);
				Assert.AreEqual(expected[i].port, actual[i].port, "index " + i);
				Assert.AreEqual(expected[i].tlsName, actual[i].tlsName, "index " + i);
			}
		}

		internal sealed class TestProvider : ISeedCandidateProvider
		{
			private readonly bool periodic;
			internal Host[] Next = [];
			internal Exception Error;
			internal int SleepMs;
			internal int Calls;

			internal TestProvider(bool periodic)
			{
				this.periodic = periodic;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				Calls++;

				if (SleepMs > 0)
				{
					Thread.Sleep(SleepMs);
				}

				if (Error != null)
				{
					throw Error;
				}

				return Next == null ? null : new List<Host>(Next);
			}

			public bool NeedsPeriodicRefresh() => periodic;

			public bool SupportsDeadline() => true;
		}
	}
}
