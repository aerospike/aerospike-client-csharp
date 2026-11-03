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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Reach package-private cluster seams for the L1 conformance runner.
	/// </summary>
	internal static class ConformanceAdapter
	{
		// Tend interval must be large enough that tendInterval/2 covers provider timeouts used by
		// L1 fixtures (default 2000ms). C# validates deadline <= tendInterval/2; Java does not.
		private const int TendInterval = 10000;

		/// <summary>
		/// Translate an advertised endpoint with the translator and miss mode the cluster selects
		/// from the policy.
		/// </summary>
		internal static Host Translate(ClientPolicy policy, Endpoint advertised)
		{
			IAddressTranslator translator = policy.addressTranslator ??
				new StaticMapAddressTranslator(policy.ipMap);
			TranslationMissMode onMiss = policy.translationOnMiss;
			return Cluster.Translate(translator, onMiss, advertised);
		}

		/// <summary>
		/// Apply each discovery result in order, the first at initialization and each later one as a
		/// refresh, and return the published seed snapshot after each.
		/// </summary>
		internal static List<Host[]> RefreshSeeds(ClientPolicy policy, Host[] staticSeeds, List<Host[]> results)
		{
			policy.tendInterval = TendInterval;
			policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;
			policy.discoveryRefreshIntervalMs = TendInterval;
			policy.discoveryTendRefreshDeadlineMs = TendInterval / 2;

			IEnumerator<Host[]> iter = results.GetEnumerator();
			ISeedCandidateProvider provider = new SequenceProvider(iter);

			Host[] snapshot = Array.Empty<Host>();
			SeedCandidateManager manager = new SeedCandidateManager(
				provider, policy, staticSeeds ?? Array.Empty<Host>(), hosts => snapshot = hosts,
				new Log.Context(""));

			List<Host[]> snapshots = new List<Host[]>(results.Count);

			try
			{
				manager.Start();
				snapshots.Add(snapshot);

				for (int tendCount = 1; tendCount < results.Count; tendCount++)
				{
					manager.Tend(tendCount, TendInterval);
					snapshots.Add(snapshot);
				}
			}
			finally
			{
				manager.Close();
			}

			return snapshots;
		}

		/// <summary>
		/// Refresh a real provider through the client's seed manager: first refresh at
		/// initialization, later ones on tend, with merge policy discovery_only and no static seeds.
		/// Call <paramref name="select"/> with the refresh index before each refresh. The first
		/// refresh must succeed.
		/// </summary>
		internal static List<Step> RefreshProvider(ISeedCandidateProvider provider, int refreshes, Action<int> select)
		{
			ClientPolicy policy = new ClientPolicy
			{
				tendInterval = TendInterval,
				seedMergePolicy = SeedMergePolicy.DISCOVERY_ONLY,
				discoveryExecutionMode = DiscoveryExecutionMode.TEND,
				discoveryRefreshIntervalMs = TendInterval,
				discoveryTendRefreshDeadlineMs = TendInterval / 2
			};

			Host[] staticSeeds = Array.Empty<Host>();
			Step last = null;
			ISeedCandidateProvider recording = new RecordingProvider(provider, step => last = step);

			Host[] snapshot = Array.Empty<Host>();
			SeedCandidateManager manager = new SeedCandidateManager(
				recording, policy, staticSeeds, hosts => snapshot = hosts, new Log.Context(""));

			List<Step> steps = new List<Step>(refreshes);

			try
			{
				select(0);
				last = null;
				manager.Start();

				if (last == null)
				{
					throw new InvalidOperationException("refresh 0 did not call the provider");
				}

				steps.Add(last.WithSeeds(snapshot));

				for (int i = 1; i < refreshes; i++)
				{
					last = null;
					select(i);
					manager.Tend(i, TendInterval);
					Step step = last;

					if (step == null)
					{
						throw new InvalidOperationException("refresh " + i + " did not call the provider");
					}

					steps.Add(step.WithSeeds(snapshot));
				}
			}
			finally
			{
				manager.Close();

				try
				{
					provider.Close();
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("provider close failed", e);
				}
			}

			return steps;
		}

		/// <summary>
		/// One provider refresh: the hosts it returned or the message it failed with, and the seed
		/// list in effect after it.
		/// </summary>
		internal sealed class Step
		{
			internal readonly List<Host> hosts;
			internal readonly string error;
			internal readonly Host[] seeds;

			internal Step(List<Host> hosts, string error)
				: this(hosts, error, null)
			{
			}

			private Step(List<Host> hosts, string error, Host[] seeds)
			{
				this.hosts = hosts;
				this.error = error;
				this.seeds = seeds;
			}

			internal Step WithSeeds(Host[] seeds)
			{
				return new Step(hosts, error, seeds);
			}
		}

		private sealed class SequenceProvider : ISeedCandidateProvider
		{
			private readonly IEnumerator<Host[]> iter;
			private bool started;

			internal SequenceProvider(IEnumerator<Host[]> iter)
			{
				this.iter = iter;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				if (!started)
				{
					started = true;

					if (!iter.MoveNext())
					{
						throw new InvalidOperationException("no discovery results");
					}
				}
				else if (!iter.MoveNext())
				{
					throw new InvalidOperationException("no more discovery results");
				}

				return new List<Host>(iter.Current ?? Array.Empty<Host>());
			}

			public bool NeedsPeriodicRefresh() => true;

			public bool SupportsDeadline() => true;
		}

		private sealed class RecordingProvider : ISeedCandidateProvider
		{
			private readonly ISeedCandidateProvider inner;
			private readonly Action<Step> onStep;

			internal RecordingProvider(ISeedCandidateProvider inner, Action<Step> onStep)
			{
				this.inner = inner;
				this.onStep = onStep;
			}

			public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
			{
				try
				{
					List<Host> hosts = inner.RefreshSeedCandidates(ctx);
					onStep(new Step(hosts, null));
					return hosts;
				}
				catch (Exception e)
				{
					onStep(new Step(null, e.Message));
					throw;
				}
			}

			public bool NeedsPeriodicRefresh() => true;

			public bool SupportsDeadline() => true;
		}
	}
}
