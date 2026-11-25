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

namespace Aerospike.Client
{
	internal sealed class MetricsExporterFailureState
	{
		internal int ConsecutiveFailures;
		internal int SuspendRemaining;
	}

	internal sealed class MetricsExporterFailureRegistry
	{
		private readonly object sync = new();
		private readonly Dictionary<IMetricsExporter, MetricsExporterFailureState>
			states = new(ReferenceEqualityComparer.Instance);

		internal MetricsExporterFailureState Get(IMetricsExporter exporter)
		{
			lock (sync)
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
		}

		internal void Clear()
		{
			lock (sync)
			{
				states.Clear();
			}
		}
	}

	internal sealed partial class MetricsManager
	{
		internal sealed class DeliveryCandidate
		{
			internal readonly MetricsReporter PreviousReporter;
			internal readonly MetricsWriter PreviousWriter;
			internal readonly MetricsReporter Reporter;
			internal readonly MetricsWriter Writer;
			internal readonly MetricsReporterConfig Config;
			internal readonly IReadOnlyList<IMetricsExporter> Targets;
			internal readonly bool ReconfigureReporter;
			internal bool OwnsReporter;
			internal bool OwnsWriter;

			internal DeliveryCandidate(
				MetricsReporter previousReporter,
				MetricsWriter previousWriter,
				MetricsReporter reporter,
				MetricsWriter writer,
				MetricsReporterConfig config,
				IEnumerable<IMetricsExporter> targets,
				bool reconfigureReporter,
				bool ownsReporter,
				bool ownsWriter)
			{
				PreviousReporter = previousReporter;
				PreviousWriter = previousWriter;
				Reporter = reporter;
				Writer = writer;
				Config = config;
				Targets = Array.AsReadOnly(
					(targets ?? Array.Empty<IMetricsExporter>()).ToArray());
				ReconfigureReporter = reconfigureReporter;
				OwnsReporter = ownsReporter;
				OwnsWriter = ownsWriter;
			}
		}

		internal void ConfigureExporters(IEnumerable<IMetricsExporter> targets)
		{
			lock (exportLock)
			{
				exporters = Array.AsReadOnly(
					(targets ?? Array.Empty<IMetricsExporter>()).ToArray());
			}
		}

		internal DeliveryCandidate PrepareDelivery(
			Cluster cluster,
			MetricsPolicy policy,
			MetricsPolicy previousPolicy)
		{
			MetricsExporterMode mode = policy.ResolveExporterMode();
			List<IMetricsExporter> targets = new();
			MetricsWriter previousWriter = FileWriter;
			MetricsWriter nextWriter = null;

			if (mode == MetricsExporterMode.File &&
				!string.IsNullOrEmpty(policy.ReportDir))
			{
				bool reuseWriter =
					previousWriter != null &&
					previousPolicy != null &&
					previousPolicy.ResolveExporterMode() ==
						MetricsExporterMode.File &&
					previousPolicy.ReportDir == policy.ReportDir &&
					previousPolicy.ReportSizeLimit ==
						policy.ReportSizeLimit &&
					previousPolicy.LatencyUnit == policy.LatencyUnit &&
					previousPolicy.LatencyColumns ==
						policy.LatencyColumns &&
					previousPolicy.LatencyShift == policy.LatencyShift;

				nextWriter = reuseWriter
					? previousWriter
					: WriterFactory(
						policy.ReportDir,
						policy.ReportSizeLimit,
						policy.LatencyColumns,
						policy.LatencyShift);
				targets.Add(nextWriter);
			}
			else if (mode == MetricsExporterMode.Custom)
			{
				targets.AddRange(policy.Exporters);
			}

			MetricsReporter previousReporter = Reporter;
			MetricsReporter nextReporter = previousReporter;
			MetricsReporterConfig config = targets.Count == 0
				? null
				: new MetricsReporterConfig(policy.ExportInterval);
			bool deliveryUnchanged =
				previousPolicy != null &&
				DeliveryEquals(previousPolicy, policy);

			if (deliveryUnchanged &&
				((targets.Count == 0 && previousReporter == null) ||
					(targets.Count > 0 && previousReporter != null &&
						(mode != MetricsExporterMode.File ||
							ReferenceEquals(previousWriter, nextWriter)))))
			{
				return new DeliveryCandidate(
					previousReporter,
					previousWriter,
					previousReporter,
					previousWriter,
					config,
					targets,
					false,
					false,
					false);
			}

			if (targets.Count == 0)
			{
				return new DeliveryCandidate(
					previousReporter,
					previousWriter,
					null,
					null,
					null,
					targets,
					false,
					false,
					false);
			}

			if (previousReporter == null)
			{
				ReporterProcessSampler.Reset();
				CompatibilityProcessSampler.Reset();
				nextReporter = new MetricsReporter(
					config,
					() => ExportCycle(cluster),
					ExportFinal);

				try
				{
					RunOutsideMetricsLock(
						cluster,
						() => ReporterStarter(nextReporter));
				}
				catch
				{
					nextReporter.DisposeNotStarted();
					if (nextWriter != null &&
						!ReferenceEquals(nextWriter, previousWriter))
					{
						RunOutsideMetricsLock(cluster, nextWriter.Dispose);
					}
					throw;
				}
			}

			return new DeliveryCandidate(
				previousReporter,
				previousWriter,
				nextReporter,
				nextWriter,
				config,
				targets,
				previousReporter != null,
				previousReporter == null,
				nextWriter != null &&
					!ReferenceEquals(nextWriter, previousWriter));
		}

		internal void QuiesceDelivery(
			Cluster cluster,
			DeliveryCandidate candidate)
		{
			if (candidate?.PreviousReporter != null)
			{
				RunOutsideMetricsLock(
					cluster,
					candidate.PreviousReporter.Quiesce);
			}
		}

		internal void CommitDelivery(
			Cluster cluster,
			DeliveryCandidate candidate)
		{
			if (candidate.ReconfigureReporter)
			{
				candidate.Reporter.Reconfigure(candidate.Config);
			}

			Reporter = candidate.Reporter;
			FileWriter = candidate.Writer;
			ConfigureExporters(candidate.Targets);
			candidate.OwnsReporter = false;
			candidate.OwnsWriter = false;

			if (candidate.PreviousReporter != null &&
				!ReferenceEquals(
					candidate.PreviousReporter,
					candidate.Reporter))
			{
				CleanupDelivery(
					cluster,
					candidate.PreviousReporter.Stop,
					"previous metrics reporter");
			}
			if (candidate.PreviousWriter != null &&
				!ReferenceEquals(
					candidate.PreviousWriter,
					candidate.Writer))
			{
				CleanupDelivery(
					cluster,
					candidate.PreviousWriter.Dispose,
					"previous metrics writer");
			}
			if (candidate.Reporter != null)
			{
				ScheduleReporterResume(cluster, candidate.Reporter);
			}
		}

		internal void AbortDelivery(
			Cluster cluster,
			DeliveryCandidate candidate,
			bool resumePrevious)
		{
			if (candidate == null)
			{
				return;
			}
			if (candidate.OwnsReporter)
			{
				CleanupDelivery(
					cluster,
					candidate.Reporter.Stop,
					"candidate metrics reporter");
				candidate.OwnsReporter = false;
			}
			if (candidate.OwnsWriter)
			{
				CleanupDelivery(
					cluster,
					candidate.Writer.Dispose,
					"candidate metrics writer");
				candidate.OwnsWriter = false;
			}
			if (resumePrevious && candidate.PreviousReporter != null)
			{
				ScheduleReporterResume(
					cluster,
					candidate.PreviousReporter);
			}
		}

		internal void StopDelivery(Cluster cluster)
		{
			MetricsReporter reporter = Reporter;
			Reporter = null;
			MetricsWriter writer = FileWriter;
			FileWriter = null;

			if (reporter != null)
			{
				RunOutsideMetricsLock(cluster, reporter.Stop);
			}
			if (writer != null)
			{
				RunOutsideMetricsLock(cluster, writer.Dispose);
			}
			ConfigureExporters(Array.Empty<IMetricsExporter>());
		}

		internal void RunOutsideMetricsLock(Cluster cluster, Action action)
		{
			if (action == null)
			{
				return;
			}

			if (!Monitor.IsEntered(cluster.metricsLock))
			{
				action();
				return;
			}

			Monitor.Exit(cluster.metricsLock);
			try
			{
				action();
			}
			finally
			{
				Monitor.Enter(cluster.metricsLock);
			}
		}

		internal void ScheduleReporterResume(
			Cluster cluster,
			MetricsReporter reporter)
		{
			ThreadPool.QueueUserWorkItem(_ =>
			{
				lock (LifecycleLock)
				{
					bool current;
					lock (cluster.metricsLock)
					{
						current = ReferenceEquals(Reporter, reporter);
					}
					if (current)
					{
						reporter.Resume();
					}
				}
			});
		}

		internal void RejectLifecycleFromExporter()
		{
			if (Reporter?.InCallback == true)
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics lifecycle methods can not be called from this client's exporter callback.");
			}
		}

		private void CleanupDelivery(
			Cluster cluster,
			Action cleanup,
			string description)
		{
			try
			{
				RunOutsideMetricsLock(cluster, cleanup);
			}
			catch (Exception e)
			{
				Log.Warn(
					"Failed to stop or dispose " +
					description +
					": " +
					Util.GetErrorMessage(e));
			}
		}

		private static bool DeliveryEquals(
			MetricsPolicy left,
			MetricsPolicy right)
		{
			if (left.ResolveExporterMode() != right.ResolveExporterMode() ||
				left.ExportInterval != right.ExportInterval ||
				left.ReportDir != right.ReportDir ||
				left.ReportSizeLimit != right.ReportSizeLimit ||
				left.LatencyUnit != right.LatencyUnit ||
				left.LatencyColumns != right.LatencyColumns ||
				left.LatencyShift != right.LatencyShift ||
				left.Exporters.Count != right.Exporters.Count)
			{
				return false;
			}

			for (int i = 0; i < left.Exporters.Count; i++)
			{
				if (!ReferenceEquals(left.Exporters[i], right.Exporters[i]))
				{
					return false;
				}
			}
			return true;
		}
	}
}
