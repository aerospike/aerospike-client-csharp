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
	/// U-1 SPI: provider failure / empty / context / close via SeedCandidateManager.
	/// </summary>
	[TestClass]
	public class TestDiscoverySpi
	{
		private static readonly Host StaticSeed = new Host("10.0.0.1", 3000);

		[TestMethod]
		public void ProviderFailureAtInitKeepsStaticSeeds()
		{
			foreach (SeedMergePolicy merge in new[] { SeedMergePolicy.MERGE, SeedMergePolicy.REPLACE, SeedMergePolicy.DISCOVERY_ONLY })
			{
				Host[] snapshot = null;
				SpiProvider provider = new SpiProvider(false)
				{
					RefreshError = new InvalidOperationException("cloud API down")
				};
				ClientPolicy policy = Policy(merge);
				SeedCandidateManager manager = new SeedCandidateManager(
					provider, policy, [StaticSeed], h => snapshot = h, Log.Context.Empty);

				manager.Start();
				Assert.AreEqual(1, snapshot.Length);
				Assert.AreEqual(StaticSeed.name, snapshot[0].name);
				Assert.AreEqual(1, provider.Contexts.Count);
				manager.Close();
			}
		}

		[TestMethod]
		public void ProviderFailureAtInitWithNoSeedFails()
		{
			SpiProvider provider = new SpiProvider(false)
			{
				RefreshError = new InvalidOperationException("cloud API down")
			};
			ClientPolicy policy = Policy(SeedMergePolicy.MERGE);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, [], _ => { }, Log.Context.Empty);

			AerospikeException ae = Assert.Throws<AerospikeException>(() => manager.Start());
			Assert.AreEqual(ResultCode.SEED_DISCOVERY_FAILED, ae.Result);
			StringAssert.Contains(ae.Message, typeof(SpiProvider).FullName);
			StringAssert.Contains(ae.Message, "cloud API down");
			manager.Close();
		}

		[TestMethod]
		public void EmptyResultWithNoSeedIsNotDiscoveryFailure()
		{
			Host[] snapshot = null;
			SpiProvider provider = new SpiProvider(false);
			ClientPolicy policy = Policy(SeedMergePolicy.MERGE);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, [], h => snapshot = h, Log.Context.Empty);

			manager.Start();
			Assert.AreEqual(0, snapshot.Length);
			manager.Close();
		}

		[TestMethod]
		public void ContextAtInitAndOnTend()
		{
			SpiProvider provider = new SpiProvider(true);
			ClientPolicy policy = Policy(SeedMergePolicy.MERGE);
			policy.clusterName = "ctx-cluster";
			policy.discoveryRefreshIntervalMs = 1000;
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, [StaticSeed, new Host("10.0.0.2", 3000)], _ => { }, Log.Context.Empty);

			manager.Start();
			manager.Tend(1, 1000);
			manager.Tend(2, 1000);

			Assert.IsTrue(provider.Contexts.Count >= 3);

			for (int i = 0; i < provider.Contexts.Count; i++)
			{
				SeedCandidateContext ctx = provider.Contexts[i];
				Assert.AreEqual("ctx-cluster", ctx.clusterName);
				Assert.AreEqual(2, ctx.configuredSeeds.Count);

				if (i == 0)
				{
					Assert.AreEqual(0, ctx.tendIteration);
				}
				else
				{
					Assert.IsTrue(ctx.tendIteration > 0);
				}
			}

			manager.Close();
		}

		[TestMethod]
		public void ContextClusterNameNullWhenNotSet()
		{
			SpiProvider provider = new SpiProvider(false);
			ClientPolicy policy = Policy(SeedMergePolicy.MERGE);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, [StaticSeed], _ => { }, Log.Context.Empty);

			manager.Start();
			Assert.IsNull(provider.Contexts[0].clusterName);
			manager.Close();
		}

		[TestMethod]
		public void CloseCalledOnceAfterManagerClose()
		{
			SpiProvider provider = new SpiProvider(false);
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, Policy(SeedMergePolicy.MERGE), [StaticSeed], _ => { }, Log.Context.Empty);

			manager.Start();
			Assert.AreEqual(0, provider.Closes);
			manager.Close();
			Assert.AreEqual(1, provider.Closes);
			Assert.AreEqual(provider.Contexts.Count, provider.CallsAtClose);
		}

		[TestMethod]
		public void CloseExceptionIsLoggedNotThrown()
		{
			SpiProvider provider = new SpiProvider(false)
			{
				CloseError = new InvalidOperationException("close blew")
			};
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, Policy(SeedMergePolicy.MERGE), [StaticSeed], _ => { }, Log.Context.Empty);

			manager.Start();
			manager.Close();
			Assert.AreEqual(1, provider.Closes);
		}

		private static ClientPolicy Policy(SeedMergePolicy merge)
		{
			return new ClientPolicy
			{
				seedMergePolicy = merge,
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				tendInterval = 1000,
				discoveryRefreshIntervalMs = 1000,
				discoveryTendRefreshDeadlineMs = 100
			};
		}

		private sealed class SpiProvider : ISeedCandidateProvider
		{
			private readonly bool periodic;
			internal readonly List<SeedCandidateContext> Contexts = new List<SeedCandidateContext>();
			internal int Closes;
			internal int CallsAtClose = -1;
			internal Exception RefreshError;
			internal Exception CloseError;

			internal SpiProvider(bool periodic)
			{
				this.periodic = periodic;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				Contexts.Add(ctx);

				if (RefreshError != null)
				{
					throw RefreshError;
				}

				return [];
			}

			public bool NeedsPeriodicRefresh() => periodic;

			public bool SupportsDeadline() => true;

			public void Close()
			{
				CallsAtClose = Contexts.Count;
				Closes++;

				if (CloseError != null)
				{
					throw CloseError;
				}
			}
		}
	}
}
