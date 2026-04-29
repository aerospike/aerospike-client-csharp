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

using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	/// <summary>
	/// Point-in-time snapshot of all client metrics. Passed to <see cref="IMetricsExporter"/>
	/// implementations on each export cycle.
	///
	/// Metrics are split into two tiers:
	/// <list type="bullet">
	///   <item><b>Standard</b> — always collected with negligible overhead (connections, threads,
	///     retryCount, etc.).</item>
	///   <item><b>Extended</b> — detailed diagnostics that carry a performance cost: CPU, memory,
	///     commandCount, and all per-namespace counters / latency histograms. Only populated
	///     when <see cref="ExtendedMetricsEnabled"/> is <c>true</c>.</item>
	/// </list>
	/// </summary>
	public sealed class MetricsSnapshot
	{
		/// <summary>
		/// When this snapshot was captured (UTC).
		/// </summary>
		public DateTime Timestamp { get; }

		/// <summary>
		/// Whether extended metrics were enabled when this snapshot was taken.
		/// Exporters must check this before reading any field marked (Extended).
		/// </summary>
		public bool ExtendedMetricsEnabled { get; }

		// ── Identifying metadata ──────────────────────────────────────────

		/// <summary>Cluster name. Empty string if not set.</summary>
		public string ClusterName { get; }

		/// <summary>Client language identifier (always "csharp").</summary>
		public string ClientType { get; }

		/// <summary>Client library version string.</summary>
		public string ClientVersion { get; }

		/// <summary>Application ID or authenticated user, if set.</summary>
		public string AppId { get; }

		/// <summary>User-defined labels from <see cref="MetricsPolicy.Labels"/>.</summary>
		public IReadOnlyDictionary<string, string> Labels { get; }

		// ── Standard cluster-level metrics ─────────────────────────────────

		/// <summary>Async worker threads currently in use.</summary>
		public int AsyncThreadsInUse { get; }

		/// <summary>Async I/O completion port threads currently in use.</summary>
		public int AsyncCompletionPortsInUse { get; }

		/// <summary>Number of commands in the connection recover queue.</summary>
		public int RecoverQueueSize { get; }

		/// <summary>Total invalid-node events since the client started.</summary>
		public int InvalidNodeCount { get; }

		/// <summary>Total command retries since the client started.</summary>
		public long RetryCount { get; }

		/// <summary>Total delay-queue timeouts since the client started.</summary>
		public long DelayQueueTimeoutCount { get; }

		// ── Extended cluster-level metrics ─────────────────────────────────

		/// <summary>Client process CPU usage percentage. (Extended)</summary>
		public double CpuPercent { get; }

		/// <summary>Client process working set memory in bytes. (Extended)</summary>
		public long MemoryBytes { get; }

		/// <summary>Total commands executed since the client started. (Extended)</summary>
		public long CommandCount { get; }

		// ── Node snapshots ─────────────────────────────────────────────────

		/// <summary>Per-node metric snapshots.</summary>
		public NodeMetricsSnapshot[] Nodes { get; }

		// ── Histogram configuration ────────────────────────────────────────

		/// <summary>Histogram configuration — number of latency buckets.</summary>
		public int LatencyColumns { get; }

		/// <summary>Histogram configuration — power-of-2 shift between bucket boundaries.</summary>
		public int LatencyShift { get; }

		public MetricsSnapshot(
			DateTime timestamp,
			bool extendedMetricsEnabled,
			string clusterName,
			string clientType,
			string clientVersion,
			string appId,
			IReadOnlyDictionary<string, string> labels,
			int asyncThreadsInUse,
			int asyncCompletionPortsInUse,
			int recoverQueueSize,
			int invalidNodeCount,
			long retryCount,
			long delayQueueTimeoutCount,
			double cpuPercent,
			long memoryBytes,
			long commandCount,
			NodeMetricsSnapshot[] nodes,
			int latencyColumns,
			int latencyShift)
		{
			Timestamp = timestamp;
			ExtendedMetricsEnabled = extendedMetricsEnabled;
			ClusterName = clusterName;
			ClientType = clientType;
			ClientVersion = clientVersion;
			AppId = appId;
			Labels = labels;
			AsyncThreadsInUse = asyncThreadsInUse;
			AsyncCompletionPortsInUse = asyncCompletionPortsInUse;
			RecoverQueueSize = recoverQueueSize;
			InvalidNodeCount = invalidNodeCount;
			RetryCount = retryCount;
			DelayQueueTimeoutCount = delayQueueTimeoutCount;
			CpuPercent = cpuPercent;
			MemoryBytes = memoryBytes;
			CommandCount = commandCount;
			Nodes = nodes;
			LatencyColumns = latencyColumns;
			LatencyShift = latencyShift;
		}
	}

	/// <summary>
	/// Metrics snapshot for a single cluster node.
	/// </summary>
	public sealed class NodeMetricsSnapshot
	{
		public string NodeName { get; }
		public string NodeAddress { get; }
		public int NodePort { get; }

		/// <summary>Standard: sync connection pool statistics.</summary>
		public ConnectionMetricsSnapshot SyncConnections { get; }

		/// <summary>Standard: async connection pool statistics.</summary>
		public ConnectionMetricsSnapshot AsyncConnections { get; }

		/// <summary>
		/// Extended: per-namespace counters and latency histograms.
		/// Empty array when extended metrics are disabled or no namespaces are tracked.
		/// </summary>
		public NamespaceMetricsSnapshot[] Namespaces { get; }

		public NodeMetricsSnapshot(
			string nodeName,
			string nodeAddress,
			int nodePort,
			ConnectionMetricsSnapshot syncConnections,
			ConnectionMetricsSnapshot asyncConnections,
			NamespaceMetricsSnapshot[] namespaces)
		{
			NodeName = nodeName;
			NodeAddress = nodeAddress;
			NodePort = nodePort;
			SyncConnections = syncConnections;
			AsyncConnections = asyncConnections;
			Namespaces = namespaces;
		}
	}

	/// <summary>
	/// Standard: connection pool statistics. Value type to avoid extra heap allocations.
	/// </summary>
	public readonly struct ConnectionMetricsSnapshot
	{
		public int InUse { get; }
		public int InPool { get; }

		/// <summary>Cumulative connections opened since node creation.</summary>
		public int Opened { get; }

		/// <summary>Cumulative connections closed since node creation.</summary>
		public int Closed { get; }

		public ConnectionMetricsSnapshot(int inUse, int inPool, int opened, int closed)
		{
			InUse = inUse;
			InPool = inPool;
			Opened = opened;
			Closed = closed;
		}

		internal static ConnectionMetricsSnapshot From(ConnectionStats stats)
		{
			return new ConnectionMetricsSnapshot(stats.inUse, stats.inPool, stats.opened, stats.closed);
		}
	}

	/// <summary>
	/// Extended: per-namespace counters and optional latency histograms for a single node.
	/// All fields in this class are extended metrics.
	/// </summary>
	public struct NamespaceMetricsSnapshot
	{
		public string Namespace { get; }

		/// <summary>Cumulative error count. (Extended)</summary>
		public long Errors { get; }

		/// <summary>Cumulative timeout count. (Extended)</summary>
		public long Timeouts { get; }

		/// <summary>Cumulative key-busy count. (Extended)</summary>
		public long KeyBusy { get; }

		/// <summary>Cumulative bytes received. (Extended)</summary>
		public long BytesIn { get; }

		/// <summary>Cumulative bytes sent. (Extended)</summary>
		public long BytesOut { get; }

		/// <summary>
		/// Latency histograms per operation type (conn, write, read, batch, query). (Extended)
		/// </summary>
		public LatencySnapshot[] Latencies { get; }

		public NamespaceMetricsSnapshot(
			string ns,
			long errors,
			long timeouts,
			long keyBusy,
			long bytesIn,
			long bytesOut,
			LatencySnapshot[] latencies)
		{
			Namespace = ns;
			Errors = errors;
			Timeouts = timeouts;
			KeyBusy = keyBusy;
			BytesIn = bytesIn;
			BytesOut = bytesOut;
			Latencies = latencies;
		}
	}

	/// <summary>
	/// Extended: latency histogram for a single operation type. Value type — the only heap
	/// allocation is the <see cref="BucketCounts"/> array.
	/// </summary>
	public readonly struct LatencySnapshot
	{
		public LatencyType Type { get; }

		/// <summary>
		/// Cumulative count per bucket. Length equals <see cref="MetricsSnapshot.LatencyColumns"/>.
		/// </summary>
		public long[] BucketCounts { get; }

		public LatencySnapshot(LatencyType type, long[] bucketCounts)
		{
			Type = type;
			BucketCounts = bucketCounts;
		}
	}
}
