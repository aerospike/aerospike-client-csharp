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

namespace Aerospike.Client
{
	internal interface IMetricsReporterScheduler : IDisposable
	{
		long Frequency { get; }
		long Timestamp { get; }
		void Set();
		void Wait(int milliseconds);
	}

	internal sealed class MetricsReporterScheduler
		: IMetricsReporterScheduler
	{
		private readonly AutoResetEvent wake = new(false);

		public long Frequency => Stopwatch.Frequency;
		public long Timestamp => Stopwatch.GetTimestamp();

		public void Set()
		{
			wake.Set();
		}

		public void Wait(int milliseconds)
		{
			wake.WaitOne(milliseconds);
		}

		public void Dispose()
		{
			wake.Dispose();
		}
	}

	internal sealed class MetricsReporterConfig
	{
		internal readonly TimeSpan Interval;
		internal readonly IReadOnlyList<IMetricsExporter> Targets;
		internal readonly bool IncludeLegacyFile;

		internal MetricsReporterConfig(
			TimeSpan interval,
			IEnumerable<IMetricsExporter> targets,
			bool includeLegacyFile)
		{
			Interval = interval;
			Targets = Array.AsReadOnly(targets.ToArray());
			IncludeLegacyFile = includeLegacyFile;
		}
	}

	internal sealed class MetricsExporterFailureState
	{
		internal int ConsecutiveFailures;
		internal int SuspendRemaining;
	}

	internal sealed class MetricsExporterFailureRegistry
	{
		private readonly Dictionary<IMetricsExporter, MetricsExporterFailureState> states =
			new(ReferenceEqualityComparer.Instance);

		internal MetricsExporterFailureState Get(IMetricsExporter exporter)
		{
			if (!states.TryGetValue(
				exporter,
				out MetricsExporterFailureState state))
			{
				state = new MetricsExporterFailureState();
				states.Add(exporter, state);
			}
			return state;
		}

		internal void Clear()
		{
			states.Clear();
		}
	}

	internal sealed class MetricsReporter
	{
		private const int FailureThreshold = 3;
		private const int SuspendedCycles = 3;

		[ThreadStatic]
		private static MetricsReporter callbackReporter;

		private sealed class FinalRequest
		{
			internal readonly MetricsSnapshot Snapshot;
			internal readonly ManualResetEventSlim Completed = new(false);
			internal bool AllSucceeded;

			internal FinalRequest(MetricsSnapshot snapshot)
			{
				Snapshot = snapshot;
			}
		}

		private readonly object sync = new();
		private readonly ManualResetEventSlim idle = new(true);
		private readonly Func<bool, MetricsSnapshot> snapshotBuilder;
		private readonly Action<MetricsSnapshot> acknowledge;
		private readonly MetricsExporterFailureRegistry failureRegistry;
		private readonly IMetricsReporterScheduler scheduler;
		private readonly Thread thread;
		private MetricsReporterConfig config;
		private long configGeneration;
		private bool quiesced = true;
		private bool stopping;
		private FinalRequest finalRequest;

		internal bool InCallback => ReferenceEquals(callbackReporter, this);
		internal bool IncludeLegacyFile
		{
			get
			{
				lock (sync)
				{
					return config.IncludeLegacyFile;
				}
			}
		}

		internal MetricsReporter(
			MetricsReporterConfig config,
			Func<bool, MetricsSnapshot> snapshotBuilder,
			Action<MetricsSnapshot> acknowledge,
			MetricsExporterFailureRegistry failureRegistry,
			IMetricsReporterScheduler scheduler = null)
		{
			this.config = config;
			this.snapshotBuilder = snapshotBuilder;
			this.acknowledge = acknowledge;
			this.failureRegistry = failureRegistry;
			this.scheduler =
				scheduler ?? new MetricsReporterScheduler();
			thread = new Thread(Run)
			{
				IsBackground = true,
				Name = "metrics-reporter"
			};
		}

		internal void Start()
		{
			thread.Start();
		}

		internal void DisposeNotStarted()
		{
			scheduler.Dispose();
			idle.Dispose();
		}

		internal void Reconfigure(MetricsReporterConfig replacement)
		{
			lock (sync)
			{
				bool intervalChanged = config.Interval != replacement.Interval;
				config = replacement;
				if (intervalChanged)
				{
					configGeneration++;
				}
			}
			scheduler.Set();
		}

		internal void Quiesce()
		{
			lock (sync)
			{
				quiesced = true;
			}
			scheduler.Set();
			idle.Wait();
		}

		internal void Resume()
		{
			lock (sync)
			{
				if (stopping || !quiesced)
				{
					return;
				}
				quiesced = false;
			}
			scheduler.Set();
		}

		internal bool ExportFinal(MetricsSnapshot snapshot)
		{
			FinalRequest request = new(snapshot);

			lock (sync)
			{
				if (stopping)
				{
					return false;
				}
				finalRequest = request;
			}
			scheduler.Set();
			request.Completed.Wait();
			return request.AllSucceeded;
		}

		internal void Stop()
		{
			lock (sync)
			{
				if (stopping)
				{
					return;
				}
				stopping = true;
			}
			scheduler.Set();
			thread.Join();
			scheduler.Dispose();
			idle.Dispose();
		}

		private void Run()
		{
			long observedGeneration = -1;
			long nextDeadline = 0;

			while (true)
			{
				MetricsReporterConfig current;
				FinalRequest final;
				bool runRegular = false;
				int waitMilliseconds = Timeout.Infinite;

				lock (sync)
				{
					if (stopping)
					{
						return;
					}

					current = config;
					final = finalRequest;

					if (final != null)
					{
						finalRequest = null;
						BeginCallback();
					}
					else if (!quiesced)
					{
						long now = scheduler.Timestamp;

						if (observedGeneration != configGeneration)
						{
							observedGeneration = configGeneration;
							nextDeadline = Add(
								now,
								current.Interval,
								scheduler.Frequency);
						}

						if (now >= nextDeadline)
						{
							BeginCallback();
							runRegular = true;
						}
						else
						{
							waitMilliseconds = GetWaitMilliseconds(
								now,
								nextDeadline,
								scheduler.Frequency);
						}
					}
				}

				if (final != null)
				{
					try
					{
						final.AllSucceeded = ExportCycle(
							final.Snapshot,
							current.Targets,
							true);
						if (final.AllSucceeded)
						{
							acknowledge(final.Snapshot);
						}
					}
					finally
					{
						EndCallback();
						final.Completed.Set();
					}
					continue;
				}

				if (runRegular)
				{
					try
					{
						MetricsSnapshot snapshot =
							snapshotBuilder(current.IncludeLegacyFile);
						bool allSucceeded = ExportCycle(
							snapshot,
							current.Targets,
							false);

						if (allSucceeded)
						{
							acknowledge(snapshot);
						}
					}
					catch (Exception e)
					{
						Log.Warn("Metrics reporter snapshot failed: " +
							Util.GetErrorMessage(e));
					}
					finally
					{
						EndCallback();

						lock (sync)
						{
							observedGeneration = configGeneration;
							nextDeadline = Add(
								scheduler.Timestamp,
								config.Interval,
								scheduler.Frequency);
						}
					}
					continue;
				}

				scheduler.Wait(waitMilliseconds);
			}
		}

		private bool ExportCycle(
			MetricsSnapshot snapshot,
			IReadOnlyList<IMetricsExporter> targets,
			bool final)
		{
			bool allSucceeded = true;

			foreach (IMetricsExporter exporter in targets)
			{
				MetricsExporterFailureState state = failureRegistry.Get(exporter);

				if (!final && state.SuspendRemaining > 0)
				{
					state.SuspendRemaining--;
					allSucceeded = false;
					continue;
				}

				try
				{
					callbackReporter = this;
					exporter.Export(snapshot);

					if (!final)
					{
						state.ConsecutiveFailures = 0;
						state.SuspendRemaining = 0;
					}
				}
				catch (Exception e)
				{
					allSucceeded = false;
					Log.Warn("Metrics exporter failed: " + Util.GetErrorMessage(e));

					if (!final)
					{
						state.ConsecutiveFailures++;

						if (state.ConsecutiveFailures >= FailureThreshold)
						{
							state.ConsecutiveFailures = 0;
							state.SuspendRemaining = SuspendedCycles;
						}
					}
				}
				finally
				{
					callbackReporter = null;
				}
			}
			return allSucceeded;
		}

		private void BeginCallback()
		{
			idle.Reset();
		}

		private void EndCallback()
		{
			lock (sync)
			{
				idle.Set();
			}
		}

		private static long Add(
			long timestamp,
			TimeSpan interval,
			long frequency)
		{
			double ticks = interval.TotalSeconds * frequency;

			if (ticks >= long.MaxValue - timestamp)
			{
				return long.MaxValue;
			}
			return timestamp + (long)Math.Ceiling(ticks);
		}

		private static int GetWaitMilliseconds(
			long now,
			long deadline,
			long frequency)
		{
			double milliseconds =
				(deadline - now) * 1000.0 / frequency;

			if (milliseconds >= int.MaxValue)
			{
				return int.MaxValue;
			}
			return Math.Max(1, (int)Math.Ceiling(milliseconds));
		}
	}
}
