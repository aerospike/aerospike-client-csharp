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
	}
}
