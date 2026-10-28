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
	/// <summary>
	/// Immutable point-in-time client metrics snapshot.
	/// </summary>
	public sealed class MetricsSnapshot
	{
		/// <summary>
		/// UTC time at which this immutable snapshot was built.
		/// </summary>
		public DateTimeOffset Timestamp { get; }

		/// <summary>
		/// True when the metrics master switch was active at capture time.
		/// A pull after disable returns false while retaining the frozen session values.
		/// </summary>
		public bool MetricsEnabled { get; }

		/// <summary>
		/// True when operational metrics were collected for this session.
		/// When false, operational counters and histograms are zero or empty.
		/// </summary>
		public bool OperationalMetricsEnabled { get; }

		/// <summary>
		/// True when API usage metrics were collected for this session.
		/// When false, all <see cref="UsageMetricsSnapshot"/> counters are zero.
		/// </summary>
		public bool UsageMetricsEnabled { get; }

		/// <summary>
		/// Non-null client implementation identifier. This client returns <c>csharp</c>.
		/// </summary>
		public string ClientType { get; }

		/// <summary>
		/// Non-null client version string.
		/// </summary>
		public string ClientVersion { get; }

		/// <summary>
		/// Non-null cluster name, or an empty string when no name is configured.
		/// </summary>
		public string ClusterName { get; }

		/// <summary>
		/// Non-null application identifier. This is the configured application id,
		/// authenticated user, or <c>not-set</c>, in that order.
		/// </summary>
		public string AppId { get; }

		/// <summary>
		/// Immutable copy of static application labels. The collection is never null.
		/// </summary>
		public IReadOnlyDictionary<string, string> Labels { get; }

		/// <summary>
		/// Unit used by every latency histogram bucket boundary in this snapshot.
		/// </summary>
		public MetricsLatencyUnit LatencyUnit { get; }

		/// <summary>
		/// Number of buckets in each non-empty latency histogram.
		/// </summary>
		public int LatencyColumns { get; }

		/// <summary>
		/// Power-of-two shift between latency bucket boundaries after the first two buckets.
		/// </summary>
		public int LatencyShift { get; }

		/// <summary>
		/// Non-null immutable cluster-level metrics.
		/// </summary>
		public ClusterMetricsSnapshot Cluster { get; }

		/// <summary>
		/// Non-null immutable client API usage metrics.
		/// </summary>
		public UsageMetricsSnapshot Usage { get; }

		/// <summary>
		/// Immutable active-node metrics ordered by node name. The collection is never null.
		/// </summary>
		public IReadOnlyList<NodeMetricsSnapshot> Nodes { get; }

		/// <summary>
		/// Immutable metrics for nodes removed since the last all-success export,
		/// ordered by node name. Pulling a snapshot does not clear this non-null collection.
		/// </summary>
		public IReadOnlyList<NodeMetricsSnapshot> NodesDeparted { get; }

		/// <summary>
		/// True when one or more departed node identities may have been omitted
		/// because the bounded retention store was full.
		/// </summary>
		public bool NodesDepartedPossiblyTruncated { get; }

		internal MetricsCompatibilitySnapshot Compatibility { get; }
		internal MetricsDepartureToken DepartureToken { get; }

		internal MetricsSnapshot(
			DateTimeOffset timestamp,
			bool metricsEnabled,
			bool operationalMetricsEnabled,
			bool usageMetricsEnabled,
			string clientType,
			string clientVersion,
			string clusterName,
			string appId,
			IReadOnlyDictionary<string, string> labels,
			MetricsLatencyUnit latencyUnit,
			int latencyColumns,
			int latencyShift,
			ClusterMetricsSnapshot cluster,
			UsageMetricsSnapshot usage,
			IEnumerable<NodeMetricsSnapshot> nodes,
			IEnumerable<NodeMetricsSnapshot> nodesDeparted,
			bool nodesDepartedPossiblyTruncated,
			MetricsCompatibilitySnapshot compatibility = null,
			MetricsDepartureToken departureToken = null)
		{
			Timestamp = timestamp;
			MetricsEnabled = metricsEnabled;
			OperationalMetricsEnabled = operationalMetricsEnabled;
			UsageMetricsEnabled = usageMetricsEnabled;
			ClientType = clientType ?? string.Empty;
			ClientVersion = clientVersion ?? string.Empty;
			ClusterName = clusterName ?? string.Empty;
			AppId = appId ?? string.Empty;
			Labels = SnapshotCollections.Copy(labels);
			LatencyUnit = latencyUnit;
			LatencyColumns = latencyColumns;
			LatencyShift = latencyShift;
			Cluster = cluster ?? ClusterMetricsSnapshot.Empty;
			Usage = usage ?? UsageMetricsSnapshot.Empty;
			Nodes = SnapshotCollections.Copy(nodes);
			NodesDeparted = SnapshotCollections.Copy(nodesDeparted);
			NodesDepartedPossiblyTruncated = nodesDepartedPossiblyTruncated;
			Compatibility = compatibility ?? MetricsCompatibilitySnapshot.Empty;
			DepartureToken = departureToken;
		}
	}

	/// <summary>
	/// Immutable cluster-level metrics.
	/// </summary>
	public sealed class ClusterMetricsSnapshot
	{
		internal static readonly ClusterMetricsSnapshot Empty = new();

		/// <summary>Current checked-out connection gauge across active nodes. Available in Tier 0.</summary>
		public int ConnectionsInUse { get; }
		/// <summary>Current idle pooled connection gauge across active nodes. Available in Tier 0.</summary>
		public int ConnectionsInPool { get; }
		/// <summary>Current active node gauge. Available in Tier 0, including before first enable.</summary>
		public int ActiveNodes { get; }
		/// <summary>Current connection recovery queue depth gauge. Available in Tier 0.</summary>
		public int RecoverQueueSize { get; }
		/// <summary>Cumulative connections opened during the metrics session. Available in Tier 0.</summary>
		public long ConnectionsOpened { get; }
		/// <summary>Cumulative connections closed during the metrics session. Available in Tier 0.</summary>
		public long ConnectionsClosed { get; }
		/// <summary>Cumulative invalid nodes observed during the metrics session. Available in Tier 0.</summary>
		public long InvalidNodes { get; }
		/// <summary>Cumulative sampled user commands. Zero when operational metrics are disabled.</summary>
		public long CommandCount { get; }
		/// <summary>Cumulative sampled command retries. Zero when operational metrics are disabled.</summary>
		public long CommandRetries { get; }
		/// <summary>Cumulative sampled client-side command timeouts. Zero when operational metrics are disabled.</summary>
		public long ClientTimeouts { get; }
		/// <summary>Cumulative sampled server-side command timeouts. Zero when operational metrics are disabled.</summary>
		public long ServerTimeouts { get; }
		/// <summary>Cumulative sampled batch calls containing a failed record. Zero when operational metrics are disabled.</summary>
		public long BatchPartialFailures { get; }
		/// <summary>
		/// Non-null successful whole-call latency histograms. A sample covers one
		/// public API call from entry through terminal success. Failed and abandoned
		/// calls do not contribute latency.
		/// </summary>
		public ClusterLatencyMetricsSnapshot Latency { get; }
		/// <summary>
		/// Non-null failures that occurred while validating a seed or peer before
		/// the connection was associated with an admitted node.
		/// </summary>
		public ValidationMetricsSnapshot Validation { get; }
		/// <summary>Cumulative connections admitted to recovery. Zero when operational metrics are disabled.</summary>
		public long RecoverEnqueued { get; }
		/// <summary>Cumulative successfully recovered connections. Zero when operational metrics are disabled.</summary>
		public long RecoverCompleted { get; }
		/// <summary>Cumulative aborted connection recoveries. Zero when operational metrics are disabled.</summary>
		public long RecoverAborted { get; }
		/// <summary>Cumulative commands timed out before wire execution. Zero when operational metrics are disabled.</summary>
		public long TimeoutImmediate { get; }
		/// <summary>Current queued query-record gauge. Zero when operational metrics are disabled.</summary>
		public long QueryRecordsQueued { get; }
		/// <summary>Current concurrent query-node-command gauge. Zero when operational metrics are disabled.</summary>
		public long QueryServersConcurrent { get; }
		/// <summary>Cumulative queries abandoned by their consumer. Zero when operational metrics are disabled.</summary>
		public long QueryAbandoned { get; }
		/// <summary>Process CPU utilization percentage since the previous pull or export sample. Zero when operational metrics are disabled.</summary>
		public double CpuPercent { get; }
		/// <summary>Current process working set in bytes. Zero when operational metrics are disabled.</summary>
		public long MemoryBytes { get; }
		/// <summary>Cumulative tend-duration histogram buckets in <see cref="MetricsSnapshot.LatencyUnit"/>. Empty when operational metrics are disabled.</summary>
		public IReadOnlyList<long> TendDuration { get; }

		internal ClusterMetricsSnapshot(
			int connectionsInUse = 0,
			int connectionsInPool = 0,
			int activeNodes = 0,
			int recoverQueueSize = 0,
			long connectionsOpened = 0,
			long connectionsClosed = 0,
			long invalidNodes = 0,
			long commandCount = 0,
			long commandRetries = 0,
			long clientTimeouts = 0,
			long serverTimeouts = 0,
			long batchPartialFailures = 0,
			ClusterLatencyMetricsSnapshot latency = null,
			ValidationMetricsSnapshot validation = null,
			long recoverEnqueued = 0,
			long recoverCompleted = 0,
			long recoverAborted = 0,
			long timeoutImmediate = 0,
			long queryRecordsQueued = 0,
			long queryServersConcurrent = 0,
			long queryAbandoned = 0,
			double cpuPercent = 0,
			long memoryBytes = 0,
			IEnumerable<long> tendDuration = null)
		{
			ConnectionsInUse = connectionsInUse;
			ConnectionsInPool = connectionsInPool;
			ActiveNodes = activeNodes;
			RecoverQueueSize = recoverQueueSize;
			ConnectionsOpened = connectionsOpened;
			ConnectionsClosed = connectionsClosed;
			InvalidNodes = invalidNodes;
			CommandCount = commandCount;
			CommandRetries = commandRetries;
			ClientTimeouts = clientTimeouts;
			ServerTimeouts = serverTimeouts;
			BatchPartialFailures = batchPartialFailures;
			Latency = latency ?? ClusterLatencyMetricsSnapshot.Empty;
			Validation = validation ?? ValidationMetricsSnapshot.Empty;
			RecoverEnqueued = recoverEnqueued;
			RecoverCompleted = recoverCompleted;
			RecoverAborted = recoverAborted;
			TimeoutImmediate = timeoutImmediate;
			QueryRecordsQueued = queryRecordsQueued;
			QueryServersConcurrent = queryServersConcurrent;
			QueryAbandoned = queryAbandoned;
			CpuPercent = cpuPercent;
			MemoryBytes = memoryBytes;
			TendDuration = SnapshotCollections.Copy(tendDuration);
		}
	}

	/// <summary>
	/// Immutable pre-admission connection validation failures.
	/// Semantic cluster-name, version, and topology rejections are excluded and
	/// are represented by <see cref="ClusterMetricsSnapshot.InvalidNodes"/>.
	/// </summary>
	public sealed class ValidationMetricsSnapshot
	{
		internal static readonly ValidationMetricsSnapshot Empty = new();

		/// <summary>Cumulative transport-open failures before node admission.</summary>
		public long OpenFailures { get; }
		/// <summary>Cumulative TLS handshake failures before node admission.</summary>
		public long TlsHandshakeFailures { get; }
		/// <summary>Cumulative authentication failures before node admission.</summary>
		public long AuthFailures { get; }

		internal ValidationMetricsSnapshot(
			long openFailures = 0,
			long tlsHandshakeFailures = 0,
			long authFailures = 0)
		{
			OpenFailures = openFailures;
			TlsHandshakeFailures = tlsHandshakeFailures;
			AuthFailures = authFailures;
		}
	}

	/// <summary>
	/// Immutable per-node connection metrics.
	/// </summary>
	public sealed class ConnectionMetricsSnapshot
	{
		internal static readonly ConnectionMetricsSnapshot Empty = new();

		/// <summary>Current checked-out connection gauge for this node. Available in Tier 0.</summary>
		public int InUse { get; }
		/// <summary>Current idle pooled connection gauge for this node. Available in Tier 0.</summary>
		public int InPool { get; }
		/// <summary>Cumulative connections opened for this node during the metrics session. Available in Tier 0.</summary>
		public long Opened { get; }
		/// <summary>Cumulative connections closed for this node during the metrics session. Available in Tier 0.</summary>
		public long Closed { get; }
		/// <summary>Cumulative pool-exhausted rejections. Zero when operational metrics are disabled.</summary>
		public long PoolExhausted { get; }
		/// <summary>Cumulative transport-open failures for this node's connection pool.</summary>
		public long OpenFailures { get; }
		/// <summary>Cumulative TLS handshake failures for this node's connection pool.</summary>
		public long TlsHandshakeFailures { get; }
		/// <summary>Cumulative authentication failures for this node's connection pool.</summary>
		public long AuthFailures { get; }

		internal ConnectionMetricsSnapshot(
			int inUse = 0,
			int inPool = 0,
			long opened = 0,
			long closed = 0,
			long poolExhausted = 0,
			long openFailures = 0,
			long tlsHandshakeFailures = 0,
			long authFailures = 0)
		{
			InUse = inUse;
			InPool = inPool;
			Opened = opened;
			Closed = closed;
			PoolExhausted = poolExhausted;
			OpenFailures = openFailures;
			TlsHandshakeFailures = tlsHandshakeFailures;
			AuthFailures = authFailures;
		}
	}

	/// <summary>
	/// Immutable metrics for one node.
	/// </summary>
	public sealed class NodeMetricsSnapshot
	{
		/// <summary>Non-null node name, or an empty string when unavailable.</summary>
		public string Name { get; }
		/// <summary>Non-null node address, or an empty string when unavailable.</summary>
		public string Address { get; }
		/// <summary>Node service port, or zero when unavailable.</summary>
		public int Port { get; }
		/// <summary>Current server-reported node error-rate gauge. Available in Tier 0.</summary>
		public int ErrorRate { get; }
		/// <summary>Non-null sync-and-async aggregate connection metrics for this node.</summary>
		public ConnectionMetricsSnapshot Connections { get; }
		/// <summary>Non-null successful connection-open latency for this node.</summary>
		public NodeLatencyMetricsSnapshot Latency { get; }
		/// <summary>Non-null immutable namespace metrics ordered by namespace name. Empty when operational metrics are disabled.</summary>
		public IReadOnlyList<NamespaceMetricsSnapshot> Namespaces { get; }

		internal NodeMetricsSnapshot(
			string name,
			string address,
			int port,
			int errorRate,
			ConnectionMetricsSnapshot connections,
			NodeLatencyMetricsSnapshot latency,
			IEnumerable<NamespaceMetricsSnapshot> namespaces)
		{
			Name = name ?? string.Empty;
			Address = address ?? string.Empty;
			Port = port;
			ErrorRate = errorRate;
			Connections = connections ?? ConnectionMetricsSnapshot.Empty;
			Latency = latency ?? NodeLatencyMetricsSnapshot.Empty;
			Namespaces = SnapshotCollections.Copy(namespaces);
		}
	}

	/// <summary>
	/// Immutable operational metrics for one namespace.
	/// </summary>
	public sealed class NamespaceMetricsSnapshot
	{
		/// <summary>Non-null namespace name. Empty means the command had no truthful namespace attribution.</summary>
		public string Name { get; }
		/// <summary>
		/// Cumulative sampled RPC-attempt and record-level errors not represented
		/// by a dedicated category. A heterogeneous batch RPC failure is counted
		/// once in every participating namespace.
		/// </summary>
		public long Errors { get; }
		/// <summary>
		/// Cumulative sampled client-side timeouts, counted once per failed RPC
		/// attempt in every participating namespace.
		/// </summary>
		public long ClientTimeouts { get; }
		/// <summary>
		/// Cumulative sampled server-side timeouts, counted once per failed RPC
		/// attempt in every participating namespace.
		/// </summary>
		public long ServerTimeouts { get; }
		/// <summary>Derived combined client and server timeout count.</summary>
		public long Timeouts => ClientTimeouts + ServerTimeouts;
		/// <summary>Cumulative sampled key-busy results.</summary>
		public long KeyBusy { get; }
		/// <summary>Cumulative sampled record-too-big results.</summary>
		public long RecordTooBig { get; }
		/// <summary>Cumulative sampled device-overload results.</summary>
		public long DeviceOverload { get; }
		/// <summary>Cumulative bytes received by successful sampled commands.</summary>
		public long BytesIn { get; }
		/// <summary>Cumulative bytes sent by successful sampled commands.</summary>
		public long BytesOut { get; }
		/// <summary>
		/// Non-null successful RPC-attempt latency histograms. For a heterogeneous
		/// batch RPC, one sample is intentionally attributed to every distinct
		/// participating namespace, so namespace values are not globally additive.
		/// </summary>
		public NamespaceLatencyMetricsSnapshot Latency { get; }

		internal NamespaceMetricsSnapshot(
			string name,
			long errors,
			long clientTimeouts,
			long serverTimeouts,
			long keyBusy,
			long recordTooBig,
			long deviceOverload,
			long bytesIn,
			long bytesOut,
			NamespaceLatencyMetricsSnapshot latency)
		{
			Name = name ?? string.Empty;
			Errors = errors;
			ClientTimeouts = clientTimeouts;
			ServerTimeouts = serverTimeouts;
			KeyBusy = keyBusy;
			RecordTooBig = recordTooBig;
			DeviceOverload = deviceOverload;
			BytesIn = bytesIn;
			BytesOut = bytesOut;
			Latency = latency ?? NamespaceLatencyMetricsSnapshot.Empty;
		}
	}

	/// <summary>
	/// Immutable successful whole-call latency histogram buckets.
	/// </summary>
	public sealed class ClusterLatencyMetricsSnapshot
	{
		internal static readonly ClusterLatencyMetricsSnapshot Empty = new();

		/// <summary>Non-null cumulative read-call latency buckets.</summary>
		public IReadOnlyList<long> Read { get; }
		/// <summary>Non-null cumulative write-call latency buckets.</summary>
		public IReadOnlyList<long> Write { get; }
		/// <summary>Non-null cumulative batch-call latency buckets.</summary>
		public IReadOnlyList<long> Batch { get; }
		/// <summary>Non-null cumulative query-call latency buckets.</summary>
		public IReadOnlyList<long> Query { get; }

		internal ClusterLatencyMetricsSnapshot(
			IEnumerable<long> read = null,
			IEnumerable<long> write = null,
			IEnumerable<long> batch = null,
			IEnumerable<long> query = null)
		{
			Read = SnapshotCollections.Copy(read);
			Write = SnapshotCollections.Copy(write);
			Batch = SnapshotCollections.Copy(batch);
			Query = SnapshotCollections.Copy(query);
		}
	}

	/// <summary>
	/// Immutable successful connection-open latency histogram buckets for one node.
	/// </summary>
	public sealed class NodeLatencyMetricsSnapshot
	{
		internal static readonly NodeLatencyMetricsSnapshot Empty = new();

		/// <summary>Non-null cumulative successful connection-open latency buckets.</summary>
		public IReadOnlyList<long> Connection { get; }

		internal NodeLatencyMetricsSnapshot(IEnumerable<long> connection = null)
		{
			Connection = SnapshotCollections.Copy(connection);
		}
	}

	/// <summary>
	/// Immutable successful RPC-attempt latency histogram buckets for one namespace.
	/// </summary>
	public sealed class NamespaceLatencyMetricsSnapshot
	{
		internal static readonly NamespaceLatencyMetricsSnapshot Empty = new();

		/// <summary>Non-null cumulative read RPC latency buckets.</summary>
		public IReadOnlyList<long> Read { get; }
		/// <summary>Non-null cumulative write RPC latency buckets.</summary>
		public IReadOnlyList<long> Write { get; }
		/// <summary>
		/// Non-null cumulative batch RPC latency buckets. A heterogeneous RPC adds
		/// one sample to every participating namespace.
		/// </summary>
		public IReadOnlyList<long> Batch { get; }
		/// <summary>Non-null cumulative query RPC latency buckets.</summary>
		public IReadOnlyList<long> Query { get; }

		internal NamespaceLatencyMetricsSnapshot(
			IEnumerable<long> read = null,
			IEnumerable<long> write = null,
			IEnumerable<long> batch = null,
			IEnumerable<long> query = null)
		{
			Read = SnapshotCollections.Copy(read);
			Write = SnapshotCollections.Copy(write);
			Batch = SnapshotCollections.Copy(batch);
			Query = SnapshotCollections.Copy(query);
		}
	}

	/// <summary>
	/// Immutable client API usage counters.
	/// </summary>
	public sealed class UsageMetricsSnapshot
	{
		internal static readonly UsageMetricsSnapshot Empty = new();

		/// <summary>Cumulative blocking API calls. Zero when usage metrics are disabled.</summary>
		public long ApiBlocking { get; }
		/// <summary>Cumulative deferred/asynchronous API calls. Zero when usage metrics are disabled.</summary>
		public long ApiDeferred { get; }
		/// <summary>Cumulative background API calls. Zero when usage metrics are disabled.</summary>
		public long ApiBackground { get; }

		internal UsageMetricsSnapshot(
			long apiBlocking = 0,
			long apiDeferred = 0,
			long apiBackground = 0)
		{
			ApiBlocking = apiBlocking;
			ApiDeferred = apiDeferred;
			ApiBackground = apiBackground;
		}
	}

	internal static class SnapshotCollections
	{
		internal static IReadOnlyList<T> Copy<T>(IEnumerable<T> source)
		{
			return source == null
				? Array.Empty<T>()
				: Array.AsReadOnly(source.ToArray());
		}

		internal static IReadOnlyDictionary<string, string> Copy(
			IReadOnlyDictionary<string, string> source)
		{
			Dictionary<string, string> copy = source == null
				? new Dictionary<string, string>(StringComparer.Ordinal)
				: new Dictionary<string, string>(source, StringComparer.Ordinal);
			return new ReadOnlyDictionary<string, string>(copy);
		}
	}

	/// <summary>
	/// Immutable compatibility data captured atomically with the public snapshot.
	/// These fields are intentionally internal because they only support the
	/// established header(2) file contract.
	/// </summary>
	internal sealed class MetricsCompatibilitySnapshot
	{
		internal static readonly MetricsCompatibilitySnapshot Empty = new(
			string.Empty,
			string.Empty,
			string.Empty,
			new ReadOnlyDictionary<string, string>(
				new Dictionary<string, string>()),
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			Array.Empty<MetricsNodeCompatibilitySnapshot>(),
			Array.Empty<MetricsNodeCompatibilitySnapshot>());

		internal readonly string ClusterName;
		internal readonly string ClientVersion;
		internal readonly string AppId;
		internal readonly IReadOnlyDictionary<string, string> Labels;
		internal readonly double CpuPercent;
		internal readonly long MemoryBytes;
		internal readonly int RecoverQueueSize;
		internal readonly int InvalidNodeCount;
		internal readonly long CommandCount;
		internal readonly long RetryCount;
		internal readonly long DelayQueueTimeoutCount;
		internal readonly int AsyncThreadsInUse;
		internal readonly int AsyncCompletionPortsInUse;
		internal readonly IReadOnlyList<MetricsNodeCompatibilitySnapshot> Nodes;
		internal readonly IReadOnlyList<MetricsNodeCompatibilitySnapshot> NodesDeparted;

		internal MetricsCompatibilitySnapshot(
			string clusterName,
			string clientVersion,
			string appId,
			IReadOnlyDictionary<string, string> labels,
			double cpuPercent,
			long memoryBytes,
			int recoverQueueSize,
			int invalidNodeCount,
			long commandCount,
			long retryCount,
			long delayQueueTimeoutCount,
			int asyncThreadsInUse,
			int asyncCompletionPortsInUse,
			IEnumerable<MetricsNodeCompatibilitySnapshot> nodes,
			IEnumerable<MetricsNodeCompatibilitySnapshot> nodesDeparted)
		{
			ClusterName = clusterName ?? string.Empty;
			ClientVersion = clientVersion ?? string.Empty;
			AppId = appId ?? string.Empty;
			Labels = SnapshotCollections.Copy(labels);
			CpuPercent = cpuPercent;
			MemoryBytes = memoryBytes;
			RecoverQueueSize = recoverQueueSize;
			InvalidNodeCount = invalidNodeCount;
			CommandCount = commandCount;
			RetryCount = retryCount;
			DelayQueueTimeoutCount = delayQueueTimeoutCount;
			AsyncThreadsInUse = asyncThreadsInUse;
			AsyncCompletionPortsInUse = asyncCompletionPortsInUse;
			Nodes = SnapshotCollections.Copy(nodes);
			NodesDeparted = SnapshotCollections.Copy(nodesDeparted);
		}
	}

	internal sealed class MetricsNodeCompatibilitySnapshot
	{
		internal readonly string Name;
		internal readonly string Address;
		internal readonly int Port;
		internal readonly ConnectionStats SyncConnections;
		internal readonly ConnectionStats AsyncConnections;
		internal readonly IReadOnlyList<MetricsNamespaceCompatibilitySnapshot> Namespaces;
		internal readonly MetricsLatencyUnit LatencyUnit;
		internal readonly int LatencyColumns;
		internal readonly int LatencyShift;

		internal MetricsNodeCompatibilitySnapshot(
			string name,
			string address,
			int port,
			ConnectionStats syncConnections,
			ConnectionStats asyncConnections,
			IEnumerable<MetricsNamespaceCompatibilitySnapshot> namespaces,
			MetricsLatencyUnit latencyUnit,
			int latencyColumns,
			int latencyShift)
		{
			Name = name ?? string.Empty;
			Address = address ?? string.Empty;
			Port = port;
			SyncConnections = syncConnections;
			AsyncConnections = asyncConnections;
			Namespaces = SnapshotCollections.Copy(namespaces);
			LatencyUnit = latencyUnit;
			LatencyColumns = latencyColumns;
			LatencyShift = latencyShift;
		}
	}

	internal sealed class MetricsNamespaceCompatibilitySnapshot
	{
		internal readonly string Name;
		internal readonly long Errors;
		internal readonly long Timeouts;
		internal readonly long KeyBusy;
		internal readonly long BytesIn;
		internal readonly long BytesOut;
		internal readonly IReadOnlyList<IReadOnlyList<long>> Latency;

		internal MetricsNamespaceCompatibilitySnapshot(
			string name,
			long errors,
			long timeouts,
			long keyBusy,
			long bytesIn,
			long bytesOut,
			IEnumerable<IReadOnlyList<long>> latency)
		{
			Name = name ?? string.Empty;
			Errors = errors;
			Timeouts = timeouts;
			KeyBusy = keyBusy;
			BytesIn = bytesIn;
			BytesOut = bytesOut;
			Latency = SnapshotCollections.Copy(latency);
		}
	}
}
