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

using System.Collections.ObjectModel;

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

	internal sealed class MetricsPublication
	{
		internal readonly MetricsStore Store;
		internal readonly MetricsRuntimeConfig Config;
		internal readonly IReadOnlyDictionary<string, string> Labels;
		internal readonly string AppId;
		internal readonly string CompatibilityAppId;

		internal MetricsPublication(
			MetricsStore store,
			MetricsRuntimeConfig config,
			IReadOnlyDictionary<string, string> labels,
			string appId,
			string compatibilityAppId)
		{
			Store = store;
			Config = config;
			Labels = labels;
			AppId = appId;
			CompatibilityAppId = compatibilityAppId;
		}
	}

	/// <summary>
	/// Owns the cluster-lifetime metrics store, runtime samplers, delivery, and
	/// transactional enable, disable, and reconfiguration lifecycle.
	/// </summary>
	internal sealed class MetricsManager
	{
		private const int FailureThreshold = 3;
		private const int SuspendedCycles = 3;

		private volatile MetricsStore store;
		private volatile MetricsPublication publication;
		private volatile MetricsCompatibilitySnapshot
			frozenCompatibilitySnapshot;
		private readonly object exportLock = new();
		internal readonly object LifecycleLock = new();
		internal MetricsReporter Reporter;
		internal MetricsWriter FileWriter;
		internal Action<MetricsReporter> ReporterStarter =
			static reporter => reporter.Start();
		internal Func<string, long, int, int, MetricsWriter> WriterFactory =
			static (dir, limit, columns, shift) =>
				new MetricsWriter(dir, limit, columns, shift);
		private IReadOnlyList<IMetricsExporter> exporters =
			Array.Empty<IMetricsExporter>();
		internal double FrozenCpuPercent;
		internal long FrozenMemoryBytes;
		internal double FrozenCompatibilityCpuPercent;
		internal long FrozenCompatibilityMemoryBytes;

		internal MetricsProcessSampler PullProcessSampler { get; } = new();

		internal MetricsProcessSampler ReporterProcessSampler { get; } = new();

		internal MetricsProcessSampler CompatibilityProcessSampler { get; } =
			new(true);

		internal MetricsExporterFailureRegistry ExporterFailures { get; } = new();

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

		internal MetricsStore Store
		{
			get => store;
			set => store = value;
		}

		internal MetricsPublication Publication
		{
			get => publication;
			set => publication = value;
		}

		internal MetricsStore CreateStore(
			MetricsPolicy policy,
			bool active = true)
		{
			return new MetricsStore(
				policy,
				active: active);
		}

		internal void Enable(Cluster cluster, MetricsPolicy policy)
		{
			RejectLifecycleFromExporter();

			lock (LifecycleLock)
			{
				if (cluster.AllowExplicitMetricsEnable())
				{
					lock (cluster.metricsLock)
					{
						EnableInternal(
							cluster,
							cluster.MergeMetricsPolicyWithConfig(policy));
						cluster.SetMetricsPolicyBase(policy);
						ExporterFailures.Clear();
					}
				}
			}
		}

		internal void Disable(Cluster cluster)
		{
			RejectLifecycleFromExporter();

			lock (LifecycleLock)
			{
				if (cluster.AllowExplicitMetricsDisable())
				{
					lock (cluster.metricsLock)
					{
						DisableInternal(cluster);
					}
				}
			}
		}

		internal void EnableConfigured(Cluster cluster, MetricsPolicy policy)
		{
			EnableInternal(cluster, policy);
		}

		internal void DisableForClose(Cluster cluster)
		{
			DisableInternal(cluster);
		}

		internal void ApplyConfiguration(
			Cluster cluster,
			MetricsPolicy candidatePolicy,
			MetricsPolicy previousPolicy,
			bool desiredEnabled,
			DeliveryCandidate deliveryCandidate,
			string publicationAppId)
		{
			if (!cluster.MetricsEnabled && desiredEnabled)
			{
				EnableCandidate(
					cluster,
					candidatePolicy,
					true,
					deliveryCandidate,
					publicationAppId,
					true);
			}
			else if (cluster.MetricsEnabled && !desiredEnabled)
			{
				DisableInternal(cluster);
				cluster.MetricsPolicy = candidatePolicy;
			}
			else if (cluster.MetricsEnabled)
			{
				if (candidatePolicy.restartRequired)
				{
					ReconfigureInternal(
						cluster,
						candidatePolicy,
						deliveryCandidate,
						publicationAppId,
						true);
				}
				else
				{
					QuiesceDelivery(cluster, deliveryCandidate);
					try
					{
						store?.ApplyPolicy(candidatePolicy);
						cluster.MetricsPolicy = candidatePolicy;
						cluster.PublishMetrics(
							store,
							candidatePolicy,
							publicationAppId,
							true);
						CommitDelivery(cluster, deliveryCandidate);
					}
					catch
					{
						store?.ApplyPolicy(previousPolicy);
						cluster.MetricsPolicy = previousPolicy;
						cluster.PublishMetrics(store, previousPolicy);
						AbortDelivery(cluster, deliveryCandidate, true);
						throw;
					}
				}
			}
			else
			{
				cluster.MetricsPolicy = candidatePolicy;
			}
		}

		private void EnableInternal(Cluster cluster, MetricsPolicy policy)
		{
			if (cluster.MetricsEnabled)
			{
				ReconfigureInternal(cluster, policy);
			}
			else
			{
				EnableCandidate(cluster, policy);
			}
		}

		private void EnableCandidate(
			Cluster cluster,
			MetricsPolicy policy,
			bool publish = true,
			DeliveryCandidate deliveryCandidate = null,
			string publicationAppId = null,
			bool overridePublicationAppId = false)
		{
			policy.Validate();
			IMetricsListener listener = policy.Listener;
			bool wasEnabled = cluster.MetricsEnabled;
			IMetricsListener previousListener = cluster.metricsListener;
			MetricsPolicy previousPolicy = cluster.MetricsPolicy;
			MetricsStore previousSession = store;
			MetricsStore candidateSession = previousSession;
			deliveryCandidate ??=
				PrepareDelivery(cluster, policy, previousPolicy);
			QuiesceDelivery(cluster, deliveryCandidate);
			if (!wasEnabled)
			{
				ResetProcessSamplers();
			}

			Node[] nodeArray = cluster.Nodes;
			NodeMetrics[] candidateNodeMetrics =
				new NodeMetrics[nodeArray.Length];

			try
			{
				for (int i = 0; i < nodeArray.Length; i++)
				{
					candidateNodeMetrics[i] =
						candidateSession.CreateNodeMetrics(
							nodeArray[i],
							policy);
				}
			}
			catch
			{
				DisposeNodeMetrics(candidateNodeMetrics);
				AbortDelivery(cluster, deliveryCandidate, true);
				throw;
			}

			bool paused = wasEnabled && previousSession?.Pause() == true;
			if (wasEnabled)
			{
				try
				{
					previousListener?.OnDisable(cluster);
				}
				catch (Exception disableException)
				{
					DisposeNodeMetrics(candidateNodeMetrics);
					try
					{
						previousListener?.OnEnable(cluster, previousPolicy);
					}
					catch (Exception rollbackException)
					{
						FailClosed(cluster, previousSession);
						throw new AggregateException(
							"Metrics listener disable and rollback both failed.",
							disableException,
							rollbackException);
					}
					if (paused)
					{
						previousSession.Resume();
					}
					AbortDelivery(cluster, deliveryCandidate, true);
					throw;
				}
			}

			try
			{
				listener?.OnEnable(cluster, policy);
			}
			catch (Exception enableException)
			{
				List<Exception> failures = [enableException];
				bool rollbackFailed = false;

				try
				{
					listener?.OnDisable(cluster);
				}
				catch (Exception cleanupException)
				{
					failures.Add(cleanupException);
				}
				DisposeNodeMetrics(candidateNodeMetrics);

				if (wasEnabled)
				{
					try
					{
						previousListener?.OnEnable(cluster, previousPolicy);
					}
					catch (Exception rollbackException)
					{
						failures.Add(rollbackException);
						rollbackFailed = true;
						FailClosed(cluster, previousSession);
					}
				}
				if (!rollbackFailed && paused)
				{
					previousSession.Resume();
				}
				AbortDelivery(
					cluster,
					deliveryCandidate,
					!rollbackFailed);
				throw failures.Count == 1
					? enableException
					: new AggregateException(
						"Metrics listener activation cleanup or rollback failed.",
						failures);
			}

			candidateSession.ApplyPolicy(policy);
			if (!wasEnabled)
			{
				candidateSession.StartSession();
			}
			for (int i = 0; i < nodeArray.Length; i++)
			{
				nodeArray[i].ReplaceMetrics(candidateNodeMetrics[i]);
			}
			cluster.metricsListener = listener;
			cluster.MetricsPolicy = policy;
			store = candidateSession;
			CommitDelivery(cluster, deliveryCandidate);
			if (publish)
			{
				cluster.PublishMetrics(
					candidateSession,
					policy,
					publicationAppId,
					overridePublicationAppId);
			}
			cluster.MetricsEnabled = true;
			if (!wasEnabled)
			{
				ClearFrozenSnapshot();
				FrozenCpuPercent = 0;
				FrozenMemoryBytes = 0;
				FrozenCompatibilityCpuPercent = 0;
				FrozenCompatibilityMemoryBytes = 0;
			}
			if (paused)
			{
				candidateSession.Resume();
			}
			cluster.MetricsPolicy.restartRequired = false;
		}

		private void DisableInternal(Cluster cluster)
		{
			if (!cluster.MetricsEnabled)
			{
				return;
			}

			MetricsStore currentStore = store;
			cluster.metricsStopping = true;
			bool paused = currentStore?.Pause() == true;
			MetricsReporter reporter = Reporter;
			if (reporter != null)
			{
				RunOutsideMetricsLock(cluster, reporter.Quiesce);
			}

			try
			{
				cluster.metricsListener?.OnDisable(cluster);
			}
			catch (Exception disableException)
			{
				try
				{
					cluster.metricsListener?.OnEnable(
						cluster,
						cluster.MetricsPolicy);
				}
				catch (Exception rollbackException)
				{
					FailClosed(cluster, currentStore);
					throw new AggregateException(
						"Metrics listener disable and rollback both failed.",
						disableException,
						rollbackException);
				}
				cluster.metricsStopping = false;
				if (reporter != null)
				{
					ScheduleReporterResume(cluster, reporter);
				}
				if (paused)
				{
					currentStore.Resume();
				}
				throw;
			}

			foreach (Node node in cluster.Nodes)
			{
				if (!node.Active)
				{
					TransferNodeDeparture(cluster, node);
				}
			}
			currentStore?.Freeze();
			foreach (Node node in cluster.Nodes)
			{
				if (node.Active)
				{
					CaptureNodeCompatibility(cluster, node, true);
				}
			}
			MetricsSnapshot finalSnapshot = BuildSnapshot(
				cluster,
				true,
				true);
			FrozenCpuPercent = finalSnapshot.Cluster.CpuPercent;
			FrozenMemoryBytes = finalSnapshot.Cluster.MemoryBytes;
			FrozenCompatibilityCpuPercent =
				finalSnapshot.Compatibility.CpuPercent;
			FrozenCompatibilityMemoryBytes =
				finalSnapshot.Compatibility.MemoryBytes;
			FreezeSnapshot(finalSnapshot);

			ExportFinalAndStop(cluster, finalSnapshot);

			foreach (Node node in cluster.Nodes)
			{
				node.DisableMetrics();
			}
			cluster.MetricsEnabled = false;
			cluster.metricsStopping = false;
		}

		private void ReconfigureInternal(
			Cluster cluster,
			MetricsPolicy policy,
			DeliveryCandidate deliveryCandidate = null,
			string publicationAppId = null,
			bool overridePublicationAppId = false)
		{
			policy.Validate();
			MetricsPolicy previousPolicy = cluster.MetricsPolicy;
			IMetricsListener previousListener = cluster.metricsListener;
			IMetricsListener listener = policy.Listener;
			MetricsStore currentStore = store;
			deliveryCandidate ??=
				PrepareDelivery(cluster, policy, previousPolicy);
			Node[] nodeArray = cluster.Nodes;
			NodeMetrics[] nodeMetrics = new NodeMetrics[nodeArray.Length];
			Histograms[] candidateHistograms =
				new Histograms[nodeArray.Length];
			Histograms[] previousHistograms =
				new Histograms[nodeArray.Length];
			bool shapeChanged =
				previousPolicy == null ||
				previousPolicy.LatencyColumns != policy.LatencyColumns ||
				previousPolicy.LatencyShift != policy.LatencyShift ||
				previousPolicy.LatencyUnit != policy.LatencyUnit;

			try
			{
				for (int i = 0; i < nodeArray.Length; i++)
				{
					nodeMetrics[i] = nodeArray[i].GetMetrics();
					if (shapeChanged && nodeMetrics[i] != null)
					{
						candidateHistograms[i] =
							NodeMetrics.CreateHistograms(policy);
					}
				}
			}
			catch
			{
				DisposeHistograms(candidateHistograms);
				AbortDelivery(cluster, deliveryCandidate, false);
				throw;
			}
			QuiesceDelivery(cluster, deliveryCandidate);

			try
			{
				previousListener?.OnDisable(cluster);
				listener?.OnEnable(cluster, policy);
			}
			catch (Exception reconfigureException)
			{
				List<Exception> failures = [reconfigureException];

				try
				{
					listener?.OnDisable(cluster);
				}
				catch (Exception cleanupException)
				{
					failures.Add(cleanupException);
				}
				DisposeHistograms(candidateHistograms);
				try
				{
					previousListener?.OnEnable(cluster, previousPolicy);
				}
				catch (Exception rollbackException)
				{
					failures.Add(rollbackException);
					FailClosed(cluster, currentStore);
				}
				AbortDelivery(
					cluster,
					deliveryCandidate,
					cluster.MetricsEnabled);
				throw failures.Count == 1
					? reconfigureException
					: new AggregateException(
						"Metrics listener reconfiguration cleanup or rollback failed.",
						failures);
			}

			try
			{
				currentStore?.ApplyPolicy(policy);
				for (int i = 0; i < nodeMetrics.Length; i++)
				{
					if (shapeChanged && nodeMetrics[i] != null)
					{
						previousHistograms[i] =
							nodeMetrics[i].ReplaceHistograms(
								candidateHistograms[i]);
						candidateHistograms[i] = null;
					}
				}
				cluster.metricsListener = listener;
				cluster.MetricsPolicy = policy;
				cluster.PublishMetrics(
					currentStore,
					policy,
					publicationAppId,
					overridePublicationAppId);
				CommitDelivery(cluster, deliveryCandidate);
				policy.restartRequired = false;
			}
			catch (Exception applyException)
			{
				DisposeHistograms(candidateHistograms);
				try
				{
					currentStore?.ApplyPolicy(previousPolicy);
					for (int i = 0; i < nodeMetrics.Length; i++)
					{
						if (previousHistograms[i] != null)
						{
							nodeMetrics[i].ReplaceHistograms(
								previousHistograms[i]);
							previousHistograms[i] = null;
						}
					}
					listener?.OnDisable(cluster);
					previousListener?.OnEnable(cluster, previousPolicy);
					cluster.PublishMetrics(currentStore, previousPolicy);
				}
				catch (Exception rollbackException)
				{
					FailClosed(cluster, currentStore);
					AbortDelivery(cluster, deliveryCandidate, false);
					throw new AggregateException(
						"Metrics reconfiguration and rollback both failed.",
						applyException,
						rollbackException);
				}
				AbortDelivery(cluster, deliveryCandidate, true);
				throw;
			}
		}

		internal void FailClosed(Cluster cluster, MetricsStore currentStore)
		{
			cluster.MetricsEnabled = false;
			cluster.metricsStopping = false;
			StopDelivery(cluster);

			foreach (Node node in cluster.Nodes)
			{
				node.DisableMetrics();
			}
			currentStore?.Freeze();
		}

		private static void DisposeHistograms(Histograms[] histograms)
		{
			foreach (Histograms histogram in histograms)
			{
				histogram?.Dispose();
			}
		}

		private static void DisposeNodeMetrics(NodeMetrics[] metrics)
		{
			foreach (NodeMetrics item in metrics)
			{
				item?.Dispose();
			}
		}

		internal MetricsCallContext BeginCall(
			Cluster cluster,
			ApiMetricsOperation operation,
			ApiExecutionMode mode)
		{
			if (operation == ApiMetricsOperation.Uninstrumented)
			{
				return default;
			}

			MetricsStore current = store;
			bool sampled = false;
			if (cluster.MetricsEnabled)
			{
				current?.BeginApiCall(
				operation,
				mode,
				out sampled);
			}
			return new MetricsCallContext(
				cluster,
				current,
				operation,
				mode,
				sampled,
				sampled);
		}

		internal void TransferNodeDeparture(
			Cluster cluster,
			Node node)
		{
			if (!node.TryBeginMetricsDepartureTransfer())
			{
				return;
			}

			try
			{
				MetricsStore departureStore =
					store ?? publication?.Store;
				MetricsNodeDeparture departure =
					departureStore?.BeginNodeDeparture(node);
				node.CloseForDeparture();

				if (departure != null)
				{
					MetricsNodeCompatibilitySnapshot compatibility =
						CaptureNodeCompatibility(
							cluster,
							node,
							true);
					departureStore.RecordNodeDeparture(
						node,
						compatibility);
				}
				node.DisposeAfterDeparture();
			}
			catch
			{
				node.ResetMetricsDepartureTransfer();
				throw;
			}
		}

		internal void ResetProcessSamplers()
		{
			PullProcessSampler.Reset();
			ReporterProcessSampler.Reset();
			CompatibilityProcessSampler.Reset();
		}

		internal void ClearFrozenSnapshot()
		{
			frozenCompatibilitySnapshot = null;
		}

		internal void FreezeSnapshot(MetricsSnapshot snapshot)
		{
			frozenCompatibilitySnapshot = snapshot?.Compatibility;
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

		internal void ExportFinalAndStop(
			Cluster cluster,
			MetricsSnapshot snapshot)
		{
			MetricsReporter reporter = Reporter;

			if (reporter != null)
			{
				RunOutsideMetricsLock(cluster, () =>
				{
					reporter.ExportFinal(snapshot);
					reporter.Stop();
				});
			}

			Reporter = null;
			MetricsWriter writer = FileWriter;
			FileWriter = null;

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

		internal void ExportCycle(Cluster cluster)
		{
			MetricsSnapshot snapshot = BuildSnapshot(
				cluster,
				null,
				true);
			ExportSnapshot(snapshot, false);
		}

		internal bool ExportFinal(MetricsSnapshot snapshot)
		{
			return ExportSnapshot(snapshot, true);
		}

		internal MetricsSnapshot BuildSnapshot(
			Cluster cluster,
			bool? metricsEnabledOverride = null,
			bool reporterSnapshot = false)
		{
			lock (cluster.metricsLock)
			{
				MetricsStoreSnapshot view = ReadStoreSnapshot(
					cluster,
					reporterSnapshot
						? ReporterProcessSampler
						: PullProcessSampler);

				if (view == null)
				{
					return BuildUncollectedSnapshot(cluster);
				}

				MetricsRuntimeView runtime = view.Runtime;
				Dictionary<string, MetricsRuntimeNodeView> runtimeNodes =
					runtime.Nodes.ToDictionary(
						node => node.Name,
						StringComparer.Ordinal);
				List<NodeMetricsSnapshot> nodes = new(runtimeNodes.Count);

				foreach (KeyValuePair<string, MetricsRuntimeNodeView> entry in
					runtimeNodes.OrderBy(
						entry => entry.Key,
						StringComparer.Ordinal))
				{
					if (!view.Nodes.TryGetValue(
						entry.Key,
						out MetricsNodeView nodeView))
					{
						nodeView = EmptyNodeView(entry.Key);
					}
					nodes.Add(BuildNodeSnapshot(
						nodeView,
						entry.Value,
						view.OperationalEnabled));
				}

				MetricsDepartureCapture departureCapture =
					view.Departures ?? MetricsDepartureCapture.Empty;
				List<MetricsDepartedNodeCapture> visibleDepartures =
					departureCapture.Nodes
						.Where(departed =>
							!runtimeNodes.ContainsKey(departed.Name))
						.ToList();
				List<NodeMetricsSnapshot> departedNodes =
					new(visibleDepartures.Count);

				foreach (MetricsDepartedNodeCapture departed in
					visibleDepartures)
				{
					departedNodes.Add(BuildNodeSnapshot(
						departed.View,
						new MetricsRuntimeNodeView(
							departed.Name,
							departed.Address,
							departed.Port,
							0,
							0,
							departed.ErrorRate,
							new ConnectionStats(0, 0, 0, 0),
							new ConnectionStats(0, 0, 0, 0)),
						view.OperationalEnabled));
				}

				ClusterMetricsSnapshot clusterSnapshot = new(
					runtime.ConnectionsInUse,
					runtime.ConnectionsInPool,
					runtime.ActiveNodes,
					runtime.RecoverQueueSize,
					view.ConnectionsOpened,
					view.ConnectionsClosed,
					view.InvalidNodes,
					view.OperationalEnabled ? view.CommandCount : 0,
					view.OperationalEnabled ? view.CommandRetries : 0,
					view.OperationalEnabled ? view.ClientTimeouts : 0,
					view.OperationalEnabled ? view.ServerTimeouts : 0,
					view.OperationalEnabled ? view.BatchPartialFailures : 0,
					view.OperationalEnabled
						? new ClusterLatencyMetricsSnapshot(
							GetLatency(
								view.CallLatency,
								Latency.LatencyType.READ),
							GetLatency(
								view.CallLatency,
								Latency.LatencyType.WRITE),
							GetLatency(
								view.CallLatency,
								Latency.LatencyType.BATCH),
							GetLatency(
								view.CallLatency,
								Latency.LatencyType.QUERY))
						: null,
					view.OperationalEnabled
						? new ValidationMetricsSnapshot(
							view.ConnectionOpenFailures,
							view.ConnectionTlsFailures,
							view.ConnectionAuthFailures)
						: null,
					view.OperationalEnabled ? view.RecoverEnqueued : 0,
					view.OperationalEnabled ? view.RecoverCompleted : 0,
					view.OperationalEnabled ? view.RecoverAborted : 0,
					view.OperationalEnabled ? view.TimeoutImmediate : 0,
					view.OperationalEnabled ? view.QueryRecordsQueued : 0,
					view.OperationalEnabled
						? view.QueryServersConcurrent
						: 0,
					view.OperationalEnabled ? view.QueryAbandoned : 0,
					view.OperationalEnabled ? runtime.CpuPercent : 0,
					view.OperationalEnabled ? runtime.MemoryBytes : 0,
					view.OperationalEnabled ? view.TendDuration : null);
				UsageMetricsSnapshot usageSnapshot = view.UsageEnabled
					? new UsageMetricsSnapshot(
						view.ApiBlocking,
						view.ApiDeferred,
						view.ApiBackground)
					: UsageMetricsSnapshot.Empty;

				return new MetricsSnapshot(
					new DateTimeOffset(
						DateTime.SpecifyKind(
							runtime.Timestamp,
							DateTimeKind.Utc)),
					metricsEnabledOverride ?? cluster.MetricsEnabled,
					view.OperationalEnabled,
					view.UsageEnabled,
					runtime.ClientType,
					runtime.ClientVersion,
					runtime.ClusterName,
					runtime.AppId,
					runtime.Labels,
					view.LatencyUnit,
					view.LatencyColumns,
					view.LatencyShift,
					clusterSnapshot,
					usageSnapshot,
					nodes,
					departedNodes,
					departureCapture.PossiblyTruncated,
					BuildCompatibilitySnapshot(
						cluster,
						visibleDepartures
							.Where(node => node.Compatibility != null)
							.Select(node => node.Compatibility),
						view),
					departureCapture.Token);
			}
		}

		internal MetricsCompatibilitySnapshot BuildCompatibilitySnapshot(
			Cluster cluster,
			IEnumerable<MetricsNodeCompatibilitySnapshot> departedNodes,
			MetricsStoreSnapshot view = null)
		{
			MetricsCompatibilitySnapshot frozen =
				frozenCompatibilitySnapshot;
			if (!cluster.MetricsEnabled && frozen != null)
			{
				return frozen;
			}

			if (view != null)
			{
				MetricsRuntimeView runtime = view.Runtime;
				List<MetricsNodeCompatibilitySnapshot> capturedNodeSnapshots =
					new(runtime.Nodes.Count);

				foreach (MetricsRuntimeNodeView node in runtime.Nodes)
				{
					MetricsNodeView nodeView = null;
					view.CompatibilityNodes?.TryGetValue(
						node.Name,
						out nodeView);
					capturedNodeSnapshots.Add(BuildCompatibilityNode(
						nodeView ?? EmptyNodeView(node.Name),
						node,
						view.LatencyUnit,
						view.LatencyColumns,
						view.LatencyShift));
				}

				return new MetricsCompatibilitySnapshot(
					runtime.ClusterName,
					runtime.ClientVersion,
					runtime.CompatibilityAppId,
					runtime.Labels,
					runtime.CompatibilityCpuPercent,
					runtime.CompatibilityMemoryBytes,
					runtime.RecoverQueueSize,
					runtime.InvalidNodes,
					view.CommandCount,
					view.CommandRetries,
					view.DelayQueueTimeouts,
					runtime.AsyncThreadsInUse,
					runtime.AsyncCompletionPortsInUse,
					capturedNodeSnapshots,
					departedNodes);
			}

			double cpuPercent;
			long memoryBytes;

			if (!cluster.MetricsEnabled &&
				publication?.Store?.AcceptsWrites == false)
			{
				cpuPercent = FrozenCompatibilityCpuPercent;
				memoryBytes = FrozenCompatibilityMemoryBytes;
			}
			else
			{
				CompatibilityProcessSampler.Read(
					out cpuPercent,
					out memoryBytes);
			}

			ThreadPool.GetMaxThreads(
				out int workerThreadsMax,
				out int completionPortThreadsMax);
			ThreadPool.GetAvailableThreads(
				out int workerThreads,
				out int completionPortThreads);
			List<MetricsNodeCompatibilitySnapshot> nodeSnapshots =
				new(cluster.Nodes.Length);

			foreach (Node node in cluster.Nodes.Where(
				node => !node.MetricsDepartureTransferred))
			{
				nodeSnapshots.Add(CaptureNodeCompatibility(cluster, node));
			}

			MetricsPublication current = publication;
			string compatibilityAppId =
				current?.CompatibilityAppId ?? cluster.appId;

			if (current == null &&
				compatibilityAppId == null &&
				cluster.user?.Length > 0)
			{
				compatibilityAppId = ByteUtil.Utf8ToString(
					cluster.user,
					0,
					cluster.user.Length);
			}
			IReadOnlyDictionary<string, string> labels =
				current?.Labels ?? cluster.MetricsPolicy?.labels;

			return new MetricsCompatibilitySnapshot(
				cluster.clusterName ?? string.Empty,
				cluster.client.clientVersion,
				compatibilityAppId ?? string.Empty,
				labels,
				cpuPercent,
				memoryBytes,
				cluster.GetRecoverQueueSize(),
				cluster.InvalidNodeCount,
				view?.CommandCount ?? cluster.GetCommandCount(),
				view?.CommandRetries ?? cluster.GetRetryCount(),
				view?.DelayQueueTimeouts ??
					cluster.GetDelayQueueTimeoutCount(),
				workerThreadsMax - workerThreads,
				completionPortThreadsMax - completionPortThreads,
				nodeSnapshots,
				departedNodes);
		}

		private MetricsSnapshot BuildUncollectedSnapshot(Cluster cluster)
		{
			Node[] nodeArray = cluster.Nodes;
			List<NodeMetricsSnapshot> nodeSnapshots =
				new(nodeArray.Length);
			int connectionsInPool = 0;
			int connectionsInUse = 0;

			foreach (Node node in nodeArray
				.Where(node => node.Active)
				.OrderBy(node => node.Name, StringComparer.Ordinal))
			{
				ConnectionStats sync = node.GetConnectionStats();
				ConnectionStats async = node is AsyncNode asyncNode
					? asyncNode.GetAsyncConnectionStats()
					: new ConnectionStats(0, 0, 0, 0);
				int inPool = sync.inPool + async.inPool;
				int inUse = sync.inUse + async.inUse;

				connectionsInPool += inPool;
				connectionsInUse += inUse;
				nodeSnapshots.Add(new NodeMetricsSnapshot(
					node.Name,
					node.address.Address.ToString(),
					node.address.Port,
					node.GetErrorRate(),
					new ConnectionMetricsSnapshot(inUse, inPool),
					NodeLatencyMetricsSnapshot.Empty,
					Array.Empty<NamespaceMetricsSnapshot>()));
			}

			Dictionary<string, string> labels =
				cluster.MetricsPolicy?.labels == null
					? new Dictionary<string, string>(StringComparer.Ordinal)
					: new Dictionary<string, string>(
						cluster.MetricsPolicy.labels,
						StringComparer.Ordinal);
			string metricsAppId = cluster.appId;

			if (string.IsNullOrEmpty(metricsAppId) &&
				cluster.user?.Length > 0)
			{
				metricsAppId = ByteUtil.Utf8ToString(
					cluster.user,
					0,
					cluster.user.Length);
			}
			if (string.IsNullOrEmpty(metricsAppId))
			{
				metricsAppId = "not-set";
			}

			MetricsPolicy policy =
				cluster.MetricsPolicy ?? new MetricsPolicy();
			return new MetricsSnapshot(
				DateTimeOffset.UtcNow,
				false,
				false,
				false,
				"csharp",
				cluster.client.clientVersion,
				cluster.clusterName ?? string.Empty,
				metricsAppId,
				labels,
				policy.LatencyUnit,
				policy.LatencyColumns,
				policy.LatencyShift,
				new ClusterMetricsSnapshot(
					connectionsInUse,
					connectionsInPool,
					nodeSnapshots.Count,
					cluster.GetRecoverQueueSize()),
				UsageMetricsSnapshot.Empty,
				nodeSnapshots,
				Array.Empty<NodeMetricsSnapshot>(),
				false,
				BuildCompatibilitySnapshot(
					cluster,
					Array.Empty<MetricsNodeCompatibilitySnapshot>()));
		}

		internal MetricsStoreSnapshot ReadStoreSnapshot(
			Cluster cluster,
			MetricsProcessSampler processSampler)
		{
			while (true)
			{
				MetricsPublication current = publication;

				if (current == null)
				{
					return null;
				}

				Node[] nodeArray = cluster.Nodes;
				List<MetricsRuntimeNodeView> nodeViews =
					new(nodeArray.Length);
				int activeNodes = 0;
				int connectionsInPool = 0;
				int connectionsInUse = 0;

				foreach (Node node in nodeArray)
				{
					if (!node.Active)
					{
						continue;
					}

					ConnectionStats sync = node.GetConnectionStats();
					ConnectionStats async = node is AsyncNode asyncNode
						? asyncNode.GetAsyncConnectionStats()
						: new ConnectionStats(0, 0, 0, 0);
					int inPool = sync.inPool + async.inPool;
					int inUse = sync.inUse + async.inUse;

					activeNodes++;
					connectionsInPool += inPool;
					connectionsInUse += inUse;
					nodeViews.Add(new MetricsRuntimeNodeView(
						node.Name,
						node.address.Address.ToString(),
						node.address.Port,
						inPool,
						inUse,
						node.GetErrorRate(),
						sync,
						async));
				}

				double cpuPercent = 0;
				long memoryBytes = 0;
				double compatibilityCpuPercent = 0;
				long compatibilityMemoryBytes = 0;

				if (current.Config.OperationalEnabled)
				{
					if (current.Store.AcceptsWrites ||
						ReferenceEquals(
							processSampler,
							ReporterProcessSampler))
					{
						(processSampler ?? PullProcessSampler).Read(
							out cpuPercent,
							out memoryBytes);
					}
					else
					{
						cpuPercent = FrozenCpuPercent;
						memoryBytes = FrozenMemoryBytes;
					}
				}
				CompatibilityProcessSampler.Read(
					out compatibilityCpuPercent,
					out compatibilityMemoryBytes);
				ThreadPool.GetMaxThreads(
					out int workerThreadsMax,
					out int completionPortThreadsMax);
				ThreadPool.GetAvailableThreads(
					out int workerThreads,
					out int completionPortThreads);
				string compatibilityAppId =
					current.CompatibilityAppId ?? cluster.appId;

				if (compatibilityAppId == null &&
					cluster.user?.Length > 0)
				{
					compatibilityAppId = ByteUtil.Utf8ToString(
						cluster.user,
						0,
						cluster.user.Length);
				}

				MetricsStoreSnapshot view =
					current.Store.Read(current.Config);

				if (view == null ||
					!ReferenceEquals(publication, current) ||
					!ReferenceEquals(cluster.Nodes, nodeArray))
				{
					continue;
				}

				return view with
				{
					Runtime = new MetricsRuntimeView(
						DateTime.UtcNow,
						"csharp",
						cluster.client.clientVersion,
						cluster.clusterName ?? string.Empty,
						current.AppId,
						compatibilityAppId ?? string.Empty,
						current.Labels,
						cpuPercent,
						memoryBytes,
						compatibilityCpuPercent,
						compatibilityMemoryBytes,
						activeNodes,
						cluster.InvalidNodeCount,
						cluster.GetRecoverQueueSize(),
						connectionsInPool,
						connectionsInUse,
						workerThreadsMax - workerThreads,
						completionPortThreadsMax - completionPortThreads,
						Array.AsReadOnly(nodeViews.ToArray())),
					Config = current.Config
				};
			}
		}

		internal MetricsNodeCompatibilitySnapshot CaptureNodeCompatibility(
			Cluster cluster,
			Node node,
			bool detachMetrics = false)
		{
			ConnectionStats syncConnections = node.GetConnectionStats();
			ConnectionStats asyncConnections = node is AsyncNode asyncNode
				? asyncNode.GetAsyncConnectionStats()
				: new ConnectionStats(0, 0, 0, 0);

			if (detachMetrics)
			{
				node.DetachMetricsAndCapture(_ => true);
			}

			MetricsStore currentStore = publication?.Store ?? store;
			MetricsNodeView view = detachMetrics
				? currentStore?.ReadCompatibilityGeneration(node)
				: currentStore?.ReadCompatibilityNode(node);
			List<MetricsNamespaceCompatibilitySnapshot> namespaces =
				BuildCompatibilityNamespaces(view);

			if (view != null)
			{
				int asyncOpened = asyncConnections.opened;
				int asyncClosed = asyncConnections.closed;
				syncConnections = new ConnectionStats(
					syncConnections.inPool,
					syncConnections.inUse,
					(int)Math.Min(
						int.MaxValue,
						Math.Max(0, view.Opened - asyncOpened)),
					(int)Math.Min(
						int.MaxValue,
						Math.Max(0, view.Closed - asyncClosed)));
			}

			Host host = node.Host;
			return new MetricsNodeCompatibilitySnapshot(
				node.Name,
				host.name,
				host.port,
				syncConnections,
				asyncConnections,
				namespaces,
				cluster.MetricsPolicy?.LatencyUnit ??
					MetricsLatencyUnit.Milliseconds,
				cluster.MetricsPolicy?.LatencyColumns ?? 7,
				cluster.MetricsPolicy?.LatencyShift ?? 1);
		}

		private static MetricsNodeCompatibilitySnapshot BuildCompatibilityNode(
			MetricsNodeView view,
			MetricsRuntimeNodeView runtime,
			MetricsLatencyUnit latencyUnit,
			int latencyColumns,
			int latencyShift)
		{
			ConnectionStats asyncConnections = runtime.AsyncConnections;
			ConnectionStats syncConnections = new(
				runtime.SyncConnections.inPool,
				runtime.SyncConnections.inUse,
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Opened - asyncConnections.opened)),
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Closed - asyncConnections.closed)));

			return new MetricsNodeCompatibilitySnapshot(
				runtime.Name,
				runtime.Address,
				runtime.Port,
				syncConnections,
				asyncConnections,
				BuildCompatibilityNamespaces(view),
				latencyUnit,
				latencyColumns,
				latencyShift);
		}

		private bool ExportSnapshot(MetricsSnapshot snapshot, bool final)
		{
			IReadOnlyList<IMetricsExporter> current;
			lock (exportLock)
			{
				current = exporters;
			}

			bool allSucceeded = true;

			foreach (IMetricsExporter exporter in current)
			{
				MetricsExporterFailureState state = ExporterFailures.Get(exporter);

				if (!final && state.SuspendRemaining > 0)
				{
					state.SuspendRemaining--;
					allSucceeded = false;
					continue;
				}

				try
				{
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
					Log.Warn("Metrics exporter failed: " +
						Util.GetErrorMessage(e));

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
			}

			if (allSucceeded)
			{
				snapshot.DepartureToken?.Store.AcknowledgeDepartures(
					snapshot.DepartureToken);
			}
			return allSucceeded;
		}

		private static MetricsNodeView EmptyNodeView(string name)
		{
			return new MetricsNodeView(
				name,
				0,
				0,
				0,
				0,
				0,
				0,
				Array.Empty<long>(),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					new Dictionary<string, MetricsNamespaceView>(
						StringComparer.Ordinal)));
		}

		private static List<MetricsNamespaceCompatibilitySnapshot>
			BuildCompatibilityNamespaces(MetricsNodeView node)
		{
			List<MetricsNamespaceCompatibilitySnapshot> namespaces = new();

			if (node == null)
			{
				return namespaces;
			}

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in
				node.Namespaces.OrderBy(
					entry => entry.Key,
					StringComparer.Ordinal))
			{
				MetricsNamespaceView ns = entry.Value;
				List<IReadOnlyList<long>> latency =
					new(Latency.GetMax());

				for (int i = 0; i < Latency.GetMax(); i++)
				{
					if ((Latency.LatencyType)i ==
						Latency.LatencyType.CONN)
					{
						latency.Add(entry.Key.Length == 0
							? node.ConnectionLatency
							: Array.AsReadOnly(
								new long[node.ConnectionLatency.Count]));
					}
					else
					{
						latency.Add(ns.Latency.TryGetValue(
							(Latency.LatencyType)i,
							out IReadOnlyList<long> buckets)
								? buckets
								: Array.Empty<long>());
					}
				}

				namespaces.Add(new MetricsNamespaceCompatibilitySnapshot(
					ns.Namespace,
					ns.Errors,
					ns.Timeouts,
					ns.KeyBusy,
					ns.BytesIn,
					ns.BytesOut,
					latency));
			}
			if (node.ConnectionLatency.Any(value => value != 0) &&
				!node.Namespaces.ContainsKey(string.Empty))
			{
				namespaces.Insert(
					0,
					new MetricsNamespaceCompatibilitySnapshot(
						string.Empty,
						0,
						0,
						0,
						0,
						0,
						Enumerable.Range(0, Latency.GetMax()).Select(index =>
							index == (int)Latency.LatencyType.CONN
								? node.ConnectionLatency
								: (IReadOnlyList<long>)Array.AsReadOnly(
									new long[node.ConnectionLatency.Count]))));
			}
			return namespaces;
		}

		private static NodeMetricsSnapshot BuildNodeSnapshot(
			MetricsNodeView node,
			MetricsRuntimeNodeView runtime,
			bool operationalEnabled)
		{
			List<NamespaceMetricsSnapshot> namespaces =
				new(node.Namespaces.Count);

			if (operationalEnabled)
			{
				foreach (KeyValuePair<string, MetricsNamespaceView> entry in
					node.Namespaces.OrderBy(
						entry => entry.Key,
						StringComparer.Ordinal))
				{
					MetricsNamespaceView ns = entry.Value;
					namespaces.Add(new NamespaceMetricsSnapshot(
						ns.Namespace,
						ns.Errors,
						ns.ClientTimeouts,
						ns.ServerTimeouts,
						ns.KeyBusy,
						ns.RecordTooBig,
						ns.DeviceOverload,
						ns.BytesIn,
						ns.BytesOut,
						new NamespaceLatencyMetricsSnapshot(
							GetLatency(
								ns,
								Latency.LatencyType.READ),
							GetLatency(
								ns,
								Latency.LatencyType.WRITE),
							GetLatency(
								ns,
								Latency.LatencyType.BATCH),
							GetLatency(
								ns,
								Latency.LatencyType.QUERY))));
				}
			}

			return new NodeMetricsSnapshot(
				node.Name,
				runtime?.Address ?? string.Empty,
				runtime?.Port ?? 0,
				runtime?.ErrorRate ?? 0,
				new ConnectionMetricsSnapshot(
					runtime?.ConnectionsInUse ?? 0,
					runtime?.ConnectionsInPool ?? 0,
					node.Opened,
					node.Closed,
					operationalEnabled ? node.PoolExhausted : 0,
					operationalEnabled ? node.OpenFailures : 0,
					operationalEnabled ? node.TlsHandshakeFailures : 0,
					operationalEnabled ? node.AuthFailures : 0),
				operationalEnabled
					? new NodeLatencyMetricsSnapshot(
						node.ConnectionLatency)
					: null,
				namespaces);
		}

		private static IReadOnlyList<long> GetLatency(
			MetricsNamespaceView ns,
			Latency.LatencyType type)
		{
			return ns.Latency.TryGetValue(
				type,
				out IReadOnlyList<long> buckets)
					? buckets
					: Array.Empty<long>();
		}

		private static IReadOnlyList<long> GetLatency(
			IReadOnlyDictionary<
				Latency.LatencyType,
				IReadOnlyList<long>> latency,
			Latency.LatencyType type)
		{
			return latency != null &&
				latency.TryGetValue(type, out IReadOnlyList<long> buckets)
					? buckets
					: Array.Empty<long>();
		}
	}
}
