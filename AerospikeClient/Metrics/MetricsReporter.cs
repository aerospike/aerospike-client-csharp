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

		internal MetricsReporterConfig(TimeSpan interval)
		{
			Interval = interval;
		}
	}

	internal sealed class MetricsReporter
	{
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
		private readonly Action exportCycle;
		private readonly Func<MetricsSnapshot, bool> finalExport;
		private readonly IMetricsReporterScheduler scheduler;
		private readonly Thread thread;
		private MetricsReporterConfig config;
		private long configGeneration;
		private bool quiesced = true;
		private bool stopping;
		private FinalRequest finalRequest;

		internal bool InCallback => ReferenceEquals(callbackReporter, this);

		internal MetricsReporter(
			MetricsReporterConfig config,
			Action exportCycle,
			Func<MetricsSnapshot, bool> finalExport,
			IMetricsReporterScheduler scheduler = null)
		{
			this.config = config;
			this.exportCycle = exportCycle;
			this.finalExport = finalExport;
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
						callbackReporter = this;
						final.AllSucceeded = finalExport(final.Snapshot);
					}
					finally
					{
						callbackReporter = null;
						EndCallback();
						final.Completed.Set();
					}
					continue;
				}

				if (runRegular)
				{
					try
					{
						callbackReporter = this;
						exportCycle();
					}
					catch (Exception e)
					{
						Log.Warn("Metrics reporter snapshot failed: " +
							Util.GetErrorMessage(e));
					}
					finally
					{
						callbackReporter = null;
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
