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
	internal sealed partial class MetricsManager
	{
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
