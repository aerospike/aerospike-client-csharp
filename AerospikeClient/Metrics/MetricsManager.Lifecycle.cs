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
	internal sealed partial class MetricsManager
	{
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
	}
}
