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
	/// CLIENT-6021: invalid hosts from a seed provider fail the refresh like an exception.
	/// </summary>
	[TestClass]
	public class TestInvalidSeedHost
	{
		private static readonly Host[] Init = [new Host("10.0.0.1", 3000)];
		private static readonly Host Valid = new Host("10.0.0.2", 3000);

		[TestMethod]
		public void InvalidPortFailsRefreshAndKeepsLastGood()
		{
			Host[] snapshot = Init;
			InvalidProvider provider = new InvalidProvider { Next = [Valid] };
			SeedCandidateManager manager = Create(provider, h => snapshot = h);

			manager.Start();
			manager.Tend(1, 1000);
			Assert.AreEqual(Valid.name, snapshot[0].name);
			Host[] lastGood = snapshot;

			provider.Next = [Valid, new Host("10.0.0.3", 65536)];
			manager.Tend(2, 1000);
			Assert.AreSame(lastGood, snapshot);

			provider.Next = [new Host("10.0.0.4", 3000)];
			manager.Tend(3, 1000);
			Assert.AreEqual("10.0.0.4", snapshot[0].name);
			manager.Close();
		}

		[TestMethod]
		public void ConfiguredSeedBypassesPortCheck()
		{
			Host badSeed = new Host("10.0.0.1", 0);
			Host[] snapshot = null;
			InvalidProvider provider = new InvalidProvider { Next = [badSeed] };
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 20,
				seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY
			};
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, [badSeed], h => snapshot = h, Log.Context.Empty);

			manager.Start();
			Assert.AreEqual(1, snapshot.Length);
			Assert.AreEqual(0, snapshot[0].port);
			manager.Close();
		}

		[TestMethod]
		public void InvalidHostAtInitWithNoSeedFails()
		{
			InvalidProvider provider = new InvalidProvider
			{
				Next = [new Host("10.0.0.1", 65536)]
			};
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 20,
				seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY
			};

			AerospikeException ex = Assert.Throws<AerospikeException>(() =>
			{
				SeedCandidateManager manager = new SeedCandidateManager(
					provider, policy, Array.Empty<Host>(), _ => { }, Log.Context.Empty);
				manager.Start();
			});
			Assert.AreEqual(ResultCode.SEED_DISCOVERY_FAILED, ex.Result);
			StringAssert.Contains(ex.Message, "returned invalid host at index 0");
		}

		[TestMethod]
		public void EmptyNameFailsRefresh()
		{
			Host[] snapshot = Init;
			InvalidProvider provider = new InvalidProvider { Next = [Valid] };
			SeedCandidateManager manager = Create(provider, h => snapshot = h);

			manager.Start();
			manager.Tend(1, 1000);
			Host[] lastGood = snapshot;

			provider.Next = [new Host("", 3000)];
			manager.Tend(2, 1000);
			Assert.AreSame(lastGood, snapshot);
			manager.Close();
		}

		private static SeedCandidateManager Create(InvalidProvider provider, Action<Host[]> publisher)
		{
			ClientPolicy policy = new ClientPolicy
			{
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 20,
				seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY
			};
			return new SeedCandidateManager(provider, policy, Init, publisher, Log.Context.Empty);
		}

		private sealed class InvalidProvider : ISeedCandidateProvider
		{
			public List<Host> Next { get; set; }
			public int Calls { get; private set; }

			public bool NeedsPeriodicRefresh() => true;

			public bool SupportsDeadline() => true;

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				Calls++;
				return Next;
			}
		}
	}
}
