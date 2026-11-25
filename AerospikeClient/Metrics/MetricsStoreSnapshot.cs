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

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	internal sealed record MetricsNamespaceView(
		string Namespace,
		long Errors,
		long Timeouts,
		long ClientTimeouts,
		long ServerTimeouts,
		long KeyBusy,
		long RecordTooBig,
		long DeviceOverload,
		long BytesIn,
		long BytesOut,
		IReadOnlyDictionary<LatencyType, IReadOnlyList<long>> Latency);

	internal sealed record MetricsNodeView(
		string Name,
		long Opened,
		long Closed,
		long PoolExhausted,
		long OpenFailures,
		long TlsHandshakeFailures,
		long AuthFailures,
		IReadOnlyList<long> ConnectionLatency,
		IReadOnlyDictionary<string, MetricsNamespaceView> Namespaces);

	internal sealed record MetricsNodeCapture(
		MetricsNodeView Public,
		MetricsNodeView Compatibility,
		bool Active);

	internal sealed class MetricsNodeGeneration
	{
		private readonly Counter errors = new();
		private readonly Counter timeouts = new();
		private readonly Counter keyBusy = new();
		private readonly object metricsLock = new();
		private NodeMetrics nodeMetrics;
		private int publicReads = 1;

		internal readonly MetricsStore Store;
		internal readonly string Name;
		internal MetricsNodeCollector Collector;

		internal MetricsNodeGeneration(
			MetricsStore store,
			string name,
			MetricsRuntimeConfig config)
		{
			Store = store;
			Name = name ?? string.Empty;
			Collector = new MetricsNodeCollector(config);
		}

		internal bool PublicReadsAllowed =>
			Volatile.Read(ref publicReads) != 0;

		internal void AddError(string ns)
		{
			errors.Increment(ns);
		}

		internal void AddTimeout(string ns)
		{
			timeouts.Increment(ns);
		}

		internal void AddKeyBusy(string ns)
		{
			keyBusy.Increment(ns);
		}

		internal long GetErrorCount(string ns = null)
		{
			return PublicReadsAllowed
				? ns == null ? errors.GetTotal() : errors.GetCountByNS(ns)
				: 0;
		}

		internal long GetTimeoutCount(string ns = null)
		{
			return PublicReadsAllowed
				? ns == null ? timeouts.GetTotal() : timeouts.GetCountByNS(ns)
				: 0;
		}

		internal long GetKeyBusyCount(string ns = null)
		{
			return PublicReadsAllowed
				? ns == null ? keyBusy.GetTotal() : keyBusy.GetCountByNS(ns)
				: 0;
		}

		internal void InvalidatePublicReads()
		{
			Volatile.Write(ref publicReads, 0);
		}

		internal void ResetOperational(MetricsRuntimeConfig config)
		{
			lock (metricsLock)
			{
				Collector = new MetricsNodeCollector(config);
				nodeMetrics = null;
			}
		}

		internal void InstallMetrics(NodeMetrics metrics)
		{
			lock (metricsLock)
			{
				nodeMetrics = metrics;
			}
		}

		internal bool OwnsMetrics(NodeMetrics metrics)
		{
			lock (metricsLock)
			{
				return ReferenceEquals(nodeMetrics, metrics);
			}
		}

		internal void DetachMetrics(
			NodeMetrics metrics,
			MetricsRuntimeConfig config)
		{
			lock (metricsLock)
			{
				if (!ReferenceEquals(nodeMetrics, metrics))
				{
					return;
				}

				Collector.Merge(metrics.Read(Name, config.Shape), config);
				nodeMetrics = null;
			}
		}

		internal MetricsNodeView Read(MetricsRuntimeConfig config)
		{
			return Read(config.Shape);
		}

		internal MetricsNodeView Read(MetricsHistogramShape shape)
		{
			return Capture(shape).Public;
		}

		internal MetricsNodeCapture Capture(MetricsHistogramShape shape)
		{
			lock (metricsLock)
			{
				MetricsHistogramShape collectorShape = Collector.CurrentShape;
				MetricsNodeView collected =
					MetricsNodeNameState.NormalizeShape(
						Collector.Read(Name, collectorShape),
						collectorShape,
						shape);

				MetricsNodeView operational = collected;

				if (nodeMetrics != null)
				{
					MetricsNodeViewAggregate aggregate = new(shape);
					aggregate.Add(collected);
					aggregate.Add(nodeMetrics.Read(Name, shape));
					operational = aggregate.ToView(Name);
				}

				return new MetricsNodeCapture(
					operational,
					BuildCompatibility(operational, shape.Columns),
					false);
			}
		}

		internal IReadOnlyList<string> GetCompatibilityNamespaces()
		{
			HashSet<string> names = new(errors.GetNamespaces(), StringComparer.Ordinal);
			names.UnionWith(timeouts.GetNamespaces());
			names.UnionWith(keyBusy.GetNamespaces());
			return Array.AsReadOnly(names.ToArray());
		}

		internal long GetHistoricalErrorCount(string ns)
		{
			return errors.GetCountByNS(ns);
		}

		internal long GetHistoricalTimeoutCount(string ns)
		{
			return timeouts.GetCountByNS(ns);
		}

		internal long GetHistoricalKeyBusyCount(string ns)
		{
			return keyBusy.GetCountByNS(ns);
		}

		internal MetricsNodeView ReadCompatibility(
			MetricsRuntimeConfig config)
		{
			return Capture(config.Shape).Compatibility;
		}

		private MetricsNodeView BuildCompatibility(
			MetricsNodeView operational,
			int columns)
		{
			Dictionary<string, MetricsNamespaceView> namespaces =
				operational.Namespaces.ToDictionary(
					entry => entry.Key,
					entry => entry.Value,
					StringComparer.Ordinal);
			HashSet<string> names = new(
				namespaces.Keys,
				StringComparer.Ordinal);
			names.UnionWith(GetCompatibilityNamespaces());

			foreach (string ns in names)
			{
				namespaces.TryGetValue(ns, out MetricsNamespaceView current);
				namespaces[ns] = new MetricsNamespaceView(
					ns,
					GetHistoricalErrorCount(ns),
					GetHistoricalTimeoutCount(ns),
					current?.ClientTimeouts ?? 0,
					current?.ServerTimeouts ?? 0,
					GetHistoricalKeyBusyCount(ns),
					current?.RecordTooBig ?? 0,
					current?.DeviceOverload ?? 0,
					current?.BytesIn ?? 0,
					current?.BytesOut ?? 0,
					current?.Latency ??
						MetricsNodeNameState.EmptyLatency(
							columns));
			}

			return operational with
			{
				Namespaces =
					new ReadOnlyDictionary<string, MetricsNamespaceView>(
						namespaces)
			};
		}
	}

	internal sealed class MetricsNodeNameState
	{
		private readonly object sync = new();
		private readonly List<MetricsNodeGeneration> generations = new();
		private readonly HashSet<MetricsNodeGeneration> active =
			new(ReferenceEqualityComparer.Instance);

		internal void Add(MetricsNodeGeneration generation)
		{
			lock (sync)
			{
				generations.Add(generation);
			}
		}

		internal void Admit(MetricsNodeGeneration generation)
		{
			lock (sync)
			{
				active.Add(generation);
			}
		}

		internal void Depart(MetricsNodeGeneration generation)
		{
			lock (sync)
			{
				active.Remove(generation);
			}
		}

		internal bool IsActive
		{
			get
			{
				lock (sync)
				{
					return active.Count != 0;
				}
			}
		}

		internal MetricsNodeView Read(
			string name,
			MetricsRuntimeConfig config)
		{
			return Read(name, config.Shape);
		}

		internal MetricsNodeView Read(
			string name,
			MetricsHistogramShape shape)
		{
			return Capture(name, shape).Public;
		}

		internal MetricsNodeCapture Capture(
			string name,
			MetricsRuntimeConfig config)
		{
			return Capture(name, config.Shape);
		}

		internal MetricsNodeCapture Capture(
			string name,
			MetricsHistogramShape shape)
		{
			lock (sync)
			{
				MetricsNodeViewAggregate publicAggregate = new(shape);
				MetricsNodeViewAggregate compatibilityAggregate = new(shape);

				foreach (MetricsNodeGeneration generation in generations)
				{
					MetricsNodeCapture capture = generation.Capture(shape);
					publicAggregate.Add(capture.Public);
					compatibilityAggregate.Add(capture.Compatibility);
				}
				return new MetricsNodeCapture(
					publicAggregate.ToView(name),
					compatibilityAggregate.ToView(name),
					active.Count != 0);
			}
		}

		internal MetricsNodeView ReadCompatibility(
			string name,
			MetricsRuntimeConfig config)
		{
			return Capture(name, config).Compatibility;
		}

		internal static IReadOnlyDictionary<LatencyType, IReadOnlyList<long>>
			EmptyLatency(int columns)
		{
			Dictionary<LatencyType, IReadOnlyList<long>> latency = new();

			for (int i = 0; i < Latency.GetMax(); i++)
			{
				latency[(LatencyType)i] =
					Array.AsReadOnly(new long[columns]);
			}
			return new ReadOnlyDictionary<
				LatencyType,
				IReadOnlyList<long>>(latency);
		}

		internal static MetricsNodeView NormalizeShape(
			MetricsNodeView source,
			MetricsHistogramShape sourceShape,
			MetricsHistogramShape targetShape)
		{
			if (sourceShape.Matches(targetShape))
			{
				return source;
			}

			Dictionary<string, MetricsNamespaceView> namespaces =
				new(StringComparer.Ordinal);

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in
				source.Namespaces)
			{
				MetricsNamespaceView ns = entry.Value;
				Dictionary<LatencyType, IReadOnlyList<long>> latency = new();

				for (int i = 0; i < Latency.GetMax(); i++)
				{
					latency[(LatencyType)i] = Array.AsReadOnly(
						new long[targetShape.Columns]);
				}

				namespaces[entry.Key] = new MetricsNamespaceView(
					ns.Namespace,
					ns.Errors,
					ns.Timeouts,
					ns.ClientTimeouts,
					ns.ServerTimeouts,
					ns.KeyBusy,
					ns.RecordTooBig,
					ns.DeviceOverload,
					ns.BytesIn,
					ns.BytesOut,
					new ReadOnlyDictionary<
						LatencyType,
						IReadOnlyList<long>>(latency));
			}

			return new MetricsNodeView(
				source.Name,
				source.Opened,
				source.Closed,
				source.PoolExhausted,
				source.OpenFailures,
				source.TlsHandshakeFailures,
				source.AuthFailures,
				Array.AsReadOnly(new long[targetShape.Columns]),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					namespaces));
		}

		internal static bool IsZero(MetricsNodeView view)
		{
			return view.Opened == 0 &&
				view.Closed == 0 &&
				view.PoolExhausted == 0 &&
				view.OpenFailures == 0 &&
				view.TlsHandshakeFailures == 0 &&
				view.AuthFailures == 0 &&
				view.ConnectionLatency.All(value => value == 0) &&
				view.Namespaces.Values.All(ns =>
					ns.Errors == 0 &&
					ns.Timeouts == 0 &&
					ns.ClientTimeouts == 0 &&
					ns.ServerTimeouts == 0 &&
					ns.KeyBusy == 0 &&
					ns.RecordTooBig == 0 &&
					ns.DeviceOverload == 0 &&
					ns.BytesIn == 0 &&
					ns.BytesOut == 0 &&
					ns.Latency.Values.All(
						buckets => buckets.All(value => value == 0)));
		}
	}

	internal sealed class MetricsNodeDeparture
	{
		internal readonly MetricsStore Store;
		internal readonly MetricsNodeCollector Collector;

		internal MetricsNodeDeparture(
			MetricsStore store,
			MetricsNodeCollector collector)
		{
			Store = store;
			Collector = collector;
		}
	}

	internal sealed record MetricsStoreSnapshot(
		long SessionId,
		DateTimeOffset StartedAt,
		long ConfigGeneration,
		long ShapeGeneration,
		bool Active,
		bool OperationalEnabled,
		bool UsageEnabled,
		MetricsLatencyUnit LatencyUnit,
		int LatencyColumns,
		int LatencyShift,
		ulong SampleRange,
		ulong SampleThreshold,
		long CommandCount,
		long CommandRetries,
		long DelayQueueTimeouts,
		long ClientTimeouts,
		long ServerTimeouts,
		long BatchPartialFailures,
		long InvalidNodes,
		long ConnectionOpenFailures,
		long ConnectionTlsFailures,
		long ConnectionAuthFailures,
		long ConnectionsOpened,
		long ConnectionsClosed,
		long RecoverEnqueued,
		long RecoverCompleted,
		long RecoverAborted,
		long TimeoutImmediate,
		long QueryRecordsQueued,
		long QueryServersConcurrent,
		long QueryAbandoned,
		IReadOnlyList<long> TendDuration,
		long ApiBlocking,
		long ApiDeferred,
		long ApiBackground,
		IReadOnlyDictionary<string, MetricsNodeView> Nodes,
		MetricsNodeView RemovedNodes)
	{
		internal MetricsRuntimeView Runtime { get; init; }
		internal MetricsRuntimeConfig Config { get; init; }
		internal IReadOnlyDictionary<string, MetricsNodeView>
			CompatibilityNodes { get; init; }
		internal MetricsDepartureCapture Departures { get; init; }
		internal IReadOnlyDictionary<LatencyType, IReadOnlyList<long>>
			CallLatency { get; init; }
	}

	internal sealed record MetricsRuntimeNodeView(
		string Name,
		string Address,
		int Port,
		int ConnectionsInPool,
		int ConnectionsInUse,
		int ErrorRate,
		ConnectionStats SyncConnections,
		ConnectionStats AsyncConnections);

	internal sealed record MetricsRuntimeView(
		DateTime Timestamp,
		string ClientType,
		string ClientVersion,
		string ClusterName,
		string AppId,
		string CompatibilityAppId,
		IReadOnlyDictionary<string, string> Labels,
		double CpuPercent,
		long MemoryBytes,
		double CompatibilityCpuPercent,
		long CompatibilityMemoryBytes,
		int ActiveNodes,
		int InvalidNodes,
		int RecoverQueueSize,
		int ConnectionsInPool,
		int ConnectionsInUse,
		int AsyncThreadsInUse,
		int AsyncCompletionPortsInUse,
		IReadOnlyList<MetricsRuntimeNodeView> Nodes);

	internal enum ConnectionFailurePhase
	{
		Open,
		Tls,
		Auth
	}

	internal sealed class MetricsDepartureWatermark
	{
		internal readonly string Name;
		internal readonly long Generation;
		internal readonly MetricsNodeView Captured;
		internal readonly MetricsHistogramShape SourceShape;

		internal MetricsDepartureWatermark(
			string name,
			long generation,
			MetricsNodeView captured,
			MetricsHistogramShape sourceShape)
		{
			Name = name;
			Generation = generation;
			Captured = captured;
			SourceShape = sourceShape;
		}
	}

	internal sealed class MetricsDepartureToken
	{
		internal readonly MetricsStore Store;
		internal readonly IReadOnlyList<MetricsDepartureWatermark> Watermarks;
		internal readonly long TruncationGeneration;

		internal MetricsDepartureToken(
			MetricsStore store,
			IEnumerable<MetricsDepartureWatermark> watermarks,
			long truncationGeneration)
		{
			Store = store;
			Watermarks = Array.AsReadOnly(watermarks.ToArray());
			TruncationGeneration = truncationGeneration;
		}
	}

	internal sealed class MetricsDepartedNodeCapture
	{
		internal readonly string Name;
		internal readonly string Address;
		internal readonly int Port;
		internal readonly int ErrorRate;
		internal readonly MetricsNodeView View;
		internal readonly MetricsNodeCompatibilitySnapshot Compatibility;

		internal MetricsDepartedNodeCapture(
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeView view,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			Name = name;
			Address = address;
			Port = port;
			ErrorRate = errorRate;
			View = view;
			Compatibility = compatibility;
		}
	}

	internal sealed class MetricsDepartureCapture
	{
		internal static readonly MetricsDepartureCapture Empty = new(
			Array.Empty<MetricsDepartedNodeCapture>(),
			false,
			null);

		internal readonly IReadOnlyList<MetricsDepartedNodeCapture> Nodes;
		internal readonly bool PossiblyTruncated;
		internal readonly MetricsDepartureToken Token;

		internal MetricsDepartureCapture(
			IEnumerable<MetricsDepartedNodeCapture> nodes,
			bool possiblyTruncated,
			MetricsDepartureToken token)
		{
			Nodes = Array.AsReadOnly(nodes.ToArray());
			PossiblyTruncated = possiblyTruncated;
			Token = token;
		}
	}
}
