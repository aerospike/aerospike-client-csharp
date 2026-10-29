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
using System.Diagnostics;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Schedule periodic seed candidate refreshes (FR-41): either a dedicated background
	/// thread (<see cref="DiscoveryExecutionMode.THREAD"/>) or the cluster tend thread
	/// (<see cref="DiscoveryExecutionMode.TEND"/>).
	/// </summary>
	internal sealed class SeedCandidateManager
	{
		internal const int MaxOverruns = 3;

		private readonly ISeedCandidateProvider provider;
		private readonly string clusterName;
		private readonly Host[] staticSeeds;
		private readonly SeedMergePolicy mergePolicy;
		private readonly DiscoveryExecutionMode executionMode;
		private readonly int refreshIntervalMs;
		private readonly int refreshTimeoutMs;
		private readonly int tendRefreshDeadlineMs;
		private readonly bool periodic;
		private readonly Action<Host[]> publisher;
		private readonly Log.Context logContext;

		private volatile Host[] snapshot = Array.Empty<Host>();
		private Host[] initSeeds = Array.Empty<Host>();
		private int overrunCount;
		private volatile bool deadlineCircuitOpen;
		private volatile bool closed;
		private volatile bool initialRefreshDone;

		private Thread refreshThread;
		private readonly AutoResetEvent wakeEvent = new AutoResetEvent(false);
		private readonly ManualResetEventSlim initialRefreshEvent = new ManualResetEventSlim(false);

		internal SeedCandidateManager(
			ISeedCandidateProvider provider,
			ClientPolicy policy,
			Host[] staticSeeds,
			Action<Host[]> publisher,
			Log.Context logContext)
		{
			this.provider = provider ?? throw new ArgumentNullException(nameof(provider));
			this.publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
			this.logContext = logContext;
			this.clusterName = policy.clusterName;
			this.staticSeeds = (Host[])(staticSeeds ?? Array.Empty<Host>()).Clone();
			this.mergePolicy = policy.seedMergePolicy;
			this.executionMode = policy.discoveryExecutionMode;
			this.refreshIntervalMs = policy.discoveryRefreshIntervalMs;
			this.refreshTimeoutMs = policy.discoveryRefreshTimeoutMs;
			this.tendRefreshDeadlineMs = policy.discoveryTendRefreshDeadlineMs;
			this.periodic = provider.NeedsPeriodicRefresh();

			if (executionMode == DiscoveryExecutionMode.TEND && periodic && !provider.SupportsDeadline())
			{
				throw new AerospikeException(
					"A seed candidate provider that needs periodic refresh must support a deadline " +
					"when discoveryExecutionMode is TEND");
			}

			if (executionMode == DiscoveryExecutionMode.THREAD && periodic && refreshTimeoutMs <= 0)
			{
				throw new AerospikeException(
					"Invalid discovery refresh timeout: " + refreshTimeoutMs + ". Must be > 0");
			}

			Validate(policy.tendInterval);
		}

		internal ISeedCandidateProvider Provider => provider;

		internal int OverrunCount => overrunCount;

		internal bool IsDeadlineCircuitOpen => deadlineCircuitOpen;

		internal bool HasRefreshThread => refreshThread != null;

		internal void Validate(int tendInterval)
		{
			if (!periodic)
			{
				return;
			}

			if (refreshIntervalMs < tendInterval)
			{
				throw new AerospikeException("Discovery refresh interval " + refreshIntervalMs +
					" must be greater or equal to the tend interval " + tendInterval);
			}

			if (executionMode != DiscoveryExecutionMode.TEND)
			{
				return;
			}

			if (tendRefreshDeadlineMs <= 0 || tendRefreshDeadlineMs > tendInterval / 2)
			{
				throw new AerospikeException("Invalid discovery tend refresh deadline: " + tendRefreshDeadlineMs +
					". Must be > 0 and <= tend interval / 2 (" + (tendInterval / 2) + ")");
			}

			if (provider is DnsSeedCandidateProvider dns)
			{
				int timeout = dns.Timeout;

				if (timeout > tendRefreshDeadlineMs)
				{
					throw new AerospikeException("DNS seed provider timeout " + timeout +
						" must be less or equal to the discovery tend refresh deadline " + tendRefreshDeadlineMs);
				}
			}

			if (provider is AwsEc2TagSeedCandidateProvider aws)
			{
				int timeout = aws.Timeout;

				if (timeout > tendRefreshDeadlineMs)
				{
					throw new AerospikeException("AWS EC2 seed provider timeout " + timeout +
						" must be less or equal to the discovery tend refresh deadline " + tendRefreshDeadlineMs);
				}
			}
		}

		/// <summary>
		/// Perform the initial refresh. In <see cref="DiscoveryExecutionMode.THREAD"/> with a
		/// periodic provider, also start the background refresh loop.
		/// </summary>
		internal void Start()
		{
			if (periodic && executionMode == DiscoveryExecutionMode.THREAD)
			{
				refreshThread = new Thread(RefreshLoop)
				{
					IsBackground = true,
					Name = "aerospike-seed-refresh"
				};
				refreshThread.Start();
				return;
			}

			Host[] discovered = RefreshOrEmpty(0, out Exception error);
			Host[] merged = Merge(discovered, true);
			PublishSnapshot(merged);
			initSeeds = merged;
			MarkInitialRefreshDone();

			if (error != null && merged.Length == 0)
			{
				throw new AerospikeException(ResultCode.SEED_DISCOVERY_FAILED,
					"Seed candidate provider " + ProviderName(provider) +
					" failed and no seed host is left: " + error.Message, error);
			}
		}

		/// <summary>
		/// In <see cref="DiscoveryExecutionMode.THREAD"/> with a periodic provider, wait for the
		/// first snapshot up to <see cref="ClientPolicy.discoveryRefreshTimeoutMs"/>.
		/// </summary>
		internal void AwaitInitialRefresh()
		{
			if (!periodic || executionMode != DiscoveryExecutionMode.THREAD)
			{
				return;
			}

			if (!initialRefreshEvent.Wait(refreshTimeoutMs))
			{
				throw new AerospikeException(ResultCode.SEED_DISCOVERY_FAILED,
					"Seed candidate provider " + ProviderName(provider) +
					" did not produce an initial snapshot within " + refreshTimeoutMs + "ms");
			}

			if (snapshot.Length == 0 && initSeeds.Length == 0)
			{
				throw new AerospikeException(ResultCode.SEED_DISCOVERY_FAILED,
					"Seed candidate provider " + ProviderName(provider) +
					" failed and no seed host is left");
			}
		}

		/// <summary>
		/// Tend or app thread. Returns the immutable snapshot. Never blocks. Never calls the provider.
		/// </summary>
		internal Host[] GetCandidates()
		{
			return snapshot;
		}

		/// <summary>
		/// Wake the background loop in <see cref="DiscoveryExecutionMode.THREAD"/> mode when the
		/// cluster has zero active nodes. No-op in <see cref="DiscoveryExecutionMode.TEND"/>.
		/// </summary>
		internal void RequestUrgentRefresh()
		{
			if (!periodic || executionMode != DiscoveryExecutionMode.THREAD || closed)
			{
				return;
			}

			wakeEvent.Set();
		}

		/// <summary>
		/// Tend-thread periodic refresh under a measured deadline
		/// (<see cref="DiscoveryExecutionMode.TEND"/> only).
		/// </summary>
		internal void Tend(int tendCount, int tendInterval)
		{
			if (!periodic || executionMode != DiscoveryExecutionMode.TEND || deadlineCircuitOpen || closed)
			{
				return;
			}

			int period = refreshIntervalMs / tendInterval;

			if (period <= 0 || tendCount % period != 0)
			{
				return;
			}

			Stopwatch sw = Stopwatch.StartNew();
			Host[] hosts = RefreshOrEmpty(tendCount, out _);
			sw.Stop();
			long elapsed = sw.ElapsedMilliseconds;

			if (elapsed > tendRefreshDeadlineMs)
			{
				int count = Interlocked.Increment(ref overrunCount);

				if (Log.WarnEnabled())
				{
					Log.Warn(logContext, "Seed candidate refresh took " + elapsed +
						"ms, exceeding tend deadline " + tendRefreshDeadlineMs +
						"ms. Consecutive overruns: " + count);
				}

				if (count >= MaxOverruns)
				{
					deadlineCircuitOpen = true;
					PublishSnapshot(initSeeds);
					Log.Error(logContext, "Seed candidate provider exceeded tend deadline " + count +
						" consecutive times. Provider disabled for this client and initial seeds restored");
				}

				return;
			}

			Interlocked.Exchange(ref overrunCount, 0);

			if (hosts != null)
			{
				Host[] seeds = Merge(hosts, false);

				if (seeds.Length > 0)
				{
					PublishSnapshot(seeds);
				}
			}
		}

		internal void Close()
		{
			closed = true;
			wakeEvent.Set();

			Thread thread = refreshThread;

			if (thread != null)
			{
				if (!thread.Join(refreshTimeoutMs > 0 ? refreshTimeoutMs : 10000) && Log.WarnEnabled())
				{
					Log.Warn(logContext, "Seed candidate refresh thread did not stop within " +
						refreshTimeoutMs + "ms");
				}

				refreshThread = null;
			}

			try
			{
				provider.Close();
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(logContext, "Seed candidate provider " + ProviderName(provider) +
						" close failed: " + e.Message);
				}
			}

			wakeEvent.Dispose();
			initialRefreshEvent.Dispose();
		}

		internal Host[] Merge(Host[] discovered, bool init)
		{
			discovered ??= Array.Empty<Host>();

			switch (mergePolicy)
			{
				case SeedMergePolicy.REPLACE:
					if (!init)
					{
						return Distinct(discovered);
					}
					return Distinct(staticSeeds, discovered);

				case SeedMergePolicy.DISCOVERY_ONLY:
					return Distinct(discovered.Length > 0 ? discovered : staticSeeds);

				default:
					return Distinct(staticSeeds, discovered);
			}
		}

		internal static string ProviderName(ISeedCandidateProvider provider)
		{
			if (provider is StaticSeedCandidateProvider)
			{
				return "static";
			}

			if (provider is DnsSeedCandidateProvider)
			{
				return "dns";
			}

			if (provider is AwsEc2TagSeedCandidateProvider)
			{
				return "aws-tags";
			}

			return provider.GetType().FullName;
		}

		internal static string TranslatorName(IAddressTranslator translator)
		{
			if (translator is PassThroughAddressTranslator)
			{
				return "pass-through";
			}

			if (translator is PortMapAddressTranslator)
			{
				return "port-map";
			}

			if (translator is StaticMapAddressTranslator)
			{
				return "static-map";
			}

			return translator.GetType().FullName;
		}

		private void RefreshLoop()
		{
			long tendIteration = 0;

			while (!closed)
			{
				bool init = !initialRefreshDone;
				Host[] discovered = RefreshOrEmpty(tendIteration++, out Exception error);
				Host[] merged = Merge(discovered, init);

				if (init)
				{
					initSeeds = merged;
					PublishSnapshot(merged);
					MarkInitialRefreshDone();

					if (error != null && merged.Length == 0 && Log.WarnEnabled())
					{
						Log.Warn(logContext, "Seed candidate provider " + ProviderName(provider) +
							" failed and no seed host is left: " + error.Message);
					}
				}
				else if (merged.Length > 0)
				{
					PublishSnapshot(merged);
				}

				if (closed)
				{
					break;
				}

				wakeEvent.WaitOne(refreshIntervalMs);
			}
		}

		private void MarkInitialRefreshDone()
		{
			initialRefreshDone = true;
			initialRefreshEvent.Set();
		}

		private Host[] RefreshOrEmpty(long tendIteration, out Exception error)
		{
			error = null;

			try
			{
				SeedCandidateContext ctx = new SeedCandidateContext(clusterName, staticSeeds, tendIteration);
				List<Host> list = provider.RefreshSeedCandidates(ctx);
				return list != null ? list.ToArray() : Array.Empty<Host>();
			}
			catch (Exception e)
			{
				error = e;

				if (Log.WarnEnabled())
				{
					Log.Warn(logContext, "Seed candidate provider " + ProviderName(provider) +
						" failed: " + e.Message);
				}

				return Array.Empty<Host>();
			}
		}

		private void PublishSnapshot(Host[] hosts)
		{
			Host[] copy = hosts ?? Array.Empty<Host>();
			snapshot = copy;

			try
			{
				publisher(copy);
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(logContext, "Seed candidate publish failed: " + e.Message);
				}
			}
		}

		private static Host[] Distinct(params Host[][] lists)
		{
			HashSet<Host> set = new HashSet<Host>();
			List<Host> result = new List<Host>();

			foreach (Host[] list in lists)
			{
				if (list == null)
				{
					continue;
				}

				foreach (Host host in list)
				{
					if (set.Add(host))
					{
						result.Add(host);
					}
				}
			}

			return result.ToArray();
		}
	}
}
