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
	/// M-2..M-8 / SeedRefresher cadence, exception keep, overrun, and trip.
	/// </summary>
	[TestClass]
	public class TestSeedCandidateManager
	{
		private static readonly Host[] Init = [new Host("10.0.0.1", 3000)];
		private static readonly Host A = new Host("10.0.0.2", 3000);
		private static readonly Host B = new Host("10.0.0.3", 3000);

		[TestMethod]
		public void CallsProviderOncePerThirtyTends()
		{
			TestSeedMergePolicy.TestProvider provider = new TestSeedMergePolicy.TestProvider(true) { Next = [A] };
			SeedCandidateManager manager = Create(provider, 30000);

			manager.Start();

			for (int tendCount = 1; tendCount <= 89; tendCount++)
			{
				manager.Tend(tendCount, 1000);
				Assert.AreEqual(1 + tendCount / 30, provider.Calls, "tendCount " + tendCount);
			}

			manager.Tend(90, 1000);
			Assert.AreEqual(4, provider.Calls);
			Assert.AreEqual(A.name, manager.GetCandidates()[0].name);
			manager.Close();
		}

		[TestMethod]
		public void ProviderThrowableKeepsSnapshot()
		{
			Host[] snapshot = Init;
			TestSeedMergePolicy.TestProvider provider = new TestSeedMergePolicy.TestProvider(true) { Next = [A] };
			SeedCandidateManager manager = Create(provider, 1000, h => snapshot = h);

			manager.Start();
			manager.Tend(1, 1000);
			Assert.AreEqual(A.name, snapshot[0].name);
			Host[] lastGood = snapshot;

			provider.Error = new InvalidOperationException("runtime");
			provider.Next = [B];
			manager.Tend(2, 1000);
			Assert.AreSame(lastGood, snapshot);

			provider.Error = null;
			provider.Next = null;
			manager.Tend(3, 1000);
			Assert.AreSame(lastGood, snapshot);

			provider.Next = [B];
			manager.Tend(4, 1000);
			Assert.AreEqual(B.name, snapshot[0].name);
			Assert.AreEqual(0, manager.OverrunCount);
			Assert.IsFalse(manager.IsDeadlineCircuitOpen);
			manager.Close();
		}

		[TestMethod]
		public void OverrunBlocksTendAndKeepsLastGood()
		{
			Host[] snapshot = Init;
			TestSeedMergePolicy.TestProvider provider = new TestSeedMergePolicy.TestProvider(true) { Next = [A] };
			SeedCandidateManager manager = Create(provider, 1000, h => snapshot = h, tendDeadlineMs: 20);

			manager.Start();
			manager.Tend(1, 1000);
			Host[] lastGood = snapshot;
			Assert.AreEqual(A.name, lastGood[0].name);

			provider.SleepMs = 50;
			provider.Next = [B];
			long begin = Environment.TickCount64;
			manager.Tend(2, 1000);
			long elapsed = Environment.TickCount64 - begin;

			Assert.IsTrue(elapsed >= 40, "tend returned after " + elapsed + "ms");
			Assert.AreSame(lastGood, snapshot);
			Assert.AreEqual(1, manager.OverrunCount);
			Assert.IsFalse(manager.IsDeadlineCircuitOpen);

			manager.Tend(3, 1000);
			Assert.AreEqual(2, manager.OverrunCount);

			provider.SleepMs = 0;
			manager.Tend(4, 1000);
			Assert.AreEqual(0, manager.OverrunCount);
			Assert.AreEqual(B.name, snapshot[0].name);
			manager.Close();
		}

		[TestMethod]
		public void ThreeConsecutiveOverrunsTrip()
		{
			Host[] snapshot = Init;
			TestSeedMergePolicy.TestProvider provider = new TestSeedMergePolicy.TestProvider(true) { Next = [A] };
			SeedCandidateManager manager = Create(provider, 1000, h => snapshot = h, tendDeadlineMs: 20);

			manager.Start();
			Host[] initSeeds = snapshot;
			manager.Tend(1, 1000);
			Assert.AreEqual(A.name, snapshot[0].name);

			provider.SleepMs = 40;

			for (int i = 1; i <= 3; i++)
			{
				manager.Tend(1 + i, 1000);
				Assert.AreEqual(i, manager.OverrunCount);
			}

			Assert.IsTrue(manager.IsDeadlineCircuitOpen);
			Assert.AreSame(initSeeds, snapshot);

			int calls = provider.Calls;

			for (int tendCount = 10; tendCount < 100; tendCount++)
			{
				manager.Tend(tendCount, 1000);
			}

			Assert.AreEqual(calls, provider.Calls);
			Assert.AreSame(initSeeds, snapshot);
			manager.Close();
		}

		[TestMethod]
		public void NoPeriodicRefreshDoesNoWork()
		{
			Host[] snapshot = Init;
			TestSeedMergePolicy.TestProvider provider = new TestSeedMergePolicy.TestProvider(false) { Next = [A] };
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 20
			};
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, Init, h => snapshot = h, Log.Context.Empty);

			manager.Start();

			for (int tendCount = 1; tendCount <= 100; tendCount++)
			{
				manager.Tend(tendCount, 1000);
			}

			Assert.AreEqual(1, provider.Calls);
			Assert.AreEqual(Init[0].name, snapshot[0].name);
			manager.Close();
		}

		[TestMethod]
		public void RejectInvalidTendRefreshDeadline()
		{
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 600
			};

			AerospikeException ex = Assert.Throws<AerospikeException>(() =>
				new SeedCandidateManager(new TestSeedMergePolicy.TestProvider(true), policy, Init, _ => { },
					Log.Context.Empty));
			StringAssert.Contains(ex.Message, "tend refresh deadline");
		}

		private static SeedCandidateManager Create(
			TestSeedMergePolicy.TestProvider provider,
			int refreshIntervalMs,
			Action<Host[]> publisher = null,
			int tendDeadlineMs = 20)
		{
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = refreshIntervalMs,
				discoveryTendRefreshDeadlineMs = tendDeadlineMs,
				seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY
			};
			return new SeedCandidateManager(
				provider, policy, Init, publisher ?? (_ => { }), Log.Context.Empty);
		}
	}
}
