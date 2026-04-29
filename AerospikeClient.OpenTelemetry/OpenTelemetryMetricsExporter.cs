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
using System.Diagnostics.Metrics;

namespace Aerospike.Client.OpenTelemetry
{
	/// <summary>
	/// OpenTelemetry-compatible metrics exporter for Aerospike client metrics.
	///
	/// All instruments are registered once on first <see cref="Export"/> call.
	/// Observable callbacks read the latest <see cref="MetricsSnapshot"/> and
	/// emit measurements only for the tier that is enabled:
	/// <list type="bullet">
	///   <item><b>Standard</b> — always emitted (connections, threads, retryCount, …).</item>
	///   <item><b>Extended</b> — only emitted when
	///     <see cref="MetricsSnapshot.ExtendedMetricsEnabled"/> is true
	///     (CPU, memory, commandCount, namespace counters, latency histograms).</item>
	/// </list>
	///
	/// <b>Resource vs. metric attributes:</b> Static identifying metadata
	/// (<c>client_type</c>, <c>client_version</c>) are exposed via
	/// <see cref="GetResourceAttributes"/> for configuration on the
	/// <c>MeterProvider</c> as OTel resource attributes. Per-measurement tags
	/// are reserved for dimensions that vary across data points
	/// (<c>cluster</c>, <c>node</c>, <c>namespace</c>, etc.).
	/// </summary>
	public class OpenTelemetryMetricsExporter : IMetricsExporter, IDisposable
	{
		/// <summary>
		/// The default meter name used when no custom meter is provided.
		/// </summary>
		public const string DefaultMeterName = "Aerospike.Client";

		private readonly Meter meter;
		private volatile bool instrumentsCreated;
		private readonly object registrationLock = new();

		// Volatile reference swap — the snapshot is fully built before publishing,
		// so readers see a consistent picture without locking.
		private volatile MetricsSnapshot latestSnapshot;

		// Pre-computed lookup tables to avoid per-scrape string allocations.
		private static readonly string[] LatencyTypeNames;
		private string[] cachedBucketBounds;
		private int cachedLatencyShift = -1;
		private int cachedLatencyColumns = -1;

		static OpenTelemetryMetricsExporter()
		{
			int max = Latency.GetMax();
			LatencyTypeNames = new string[max];
			for (int i = 0; i < max; i++)
			{
				LatencyTypeNames[i] = Latency.LatencyTypeToString((Latency.LatencyType)i);
			}
		}

		/// <summary>
		/// Returns resource-level attributes that identify the Aerospike client.
		/// Configure these on the <c>MeterProvider</c> via <c>ConfigureResource()</c>
		/// so they appear once as OTel resource attributes rather than being
		/// duplicated on every measurement.
		/// </summary>
		/// <param name="clusterName">
		/// Cluster name from <see cref="AerospikeClient.ClusterName"/> (available after connect).
		/// </param>
		public static KeyValuePair<string, object>[] GetResourceAttributes(string clusterName)
		{
			return new[]
			{
				new KeyValuePair<string, object>("client_type", "csharp"),
				new KeyValuePair<string, object>("client_version",
					typeof(MetricsSnapshot).Assembly.GetName().Version?.ToString() ?? "unknown"),
				new KeyValuePair<string, object>("cluster", clusterName ?? ""),
			};
		}

		/// <summary>
		/// Create a new OpenTelemetry metrics exporter using the provided meter.
		/// The caller is responsible for the Meter lifecycle via MeterProvider.
		/// </summary>
		public OpenTelemetryMetricsExporter(Meter meter)
		{
			this.meter = meter ?? throw new ArgumentNullException(nameof(meter));
		}

		/// <summary>
		/// Export a metrics snapshot. Creates OTel instruments on first call, then
		/// updates the latest snapshot reference that observable callbacks read from.
		/// </summary>
		public void Export(MetricsSnapshot snapshot)
		{
			if (!instrumentsCreated)
			{
				lock (registrationLock)
				{
					if (!instrumentsCreated)
					{
						CreateInstruments();
						instrumentsCreated = true;
					}
				}
			}

			latestSnapshot = snapshot;
		}

		public void Dispose()
		{
			latestSnapshot = null;
		}

		#region Instrument Creation

		private void CreateInstruments()
		{
			// ── Standard cluster-level gauges ──────────────────────────────
			CreateGauge(MetricDescriptors.AsyncThreadsInUse,
				() => ObserveClusterGauge(s => s.AsyncThreadsInUse));

			CreateGauge(MetricDescriptors.AsyncCompletionPortsInUse,
				() => ObserveClusterGauge(s => s.AsyncCompletionPortsInUse));

			CreateGauge(MetricDescriptors.RecoverQueueSize,
				() => ObserveClusterGauge(s => s.RecoverQueueSize));

			// ── Standard cluster-level counters ───────────────────────────
			CreateCounter(MetricDescriptors.InvalidNodeCount,
				() => ObserveClusterCounter(s => s.InvalidNodeCount));

			CreateCounter(MetricDescriptors.RetryCount,
				() => ObserveClusterCounter(s => s.RetryCount));

			CreateCounter(MetricDescriptors.DelayQueueTimeoutCount,
				() => ObserveClusterCounter(s => s.DelayQueueTimeoutCount));

			// ── Extended cluster-level gauges ──────────────────────────────
			CreateGauge(MetricDescriptors.CpuPercent,
				() => ObserveExtendedClusterGauge(s => s.CpuPercent));

			CreateGauge(MetricDescriptors.MemoryBytes,
				() => ObserveExtendedClusterGauge(s => s.MemoryBytes));

			// ── Extended cluster-level counters ────────────────────────────
			CreateCounter(MetricDescriptors.CommandCount,
				() => ObserveExtendedClusterCounter(s => s.CommandCount));

			// ── Standard node connection gauges ────────────────────────────
			CreateGauge(MetricDescriptors.ConnectionsInUse,
				() => ObserveConnectionGauge(c => c.InUse));

			CreateGauge(MetricDescriptors.ConnectionsInPool,
				() => ObserveConnectionGauge(c => c.InPool));

			// ── Standard node connection counters ─────────────────────────
			CreateCounter(MetricDescriptors.ConnectionsOpened,
				() => ObserveConnectionCounter(c => c.Opened));

			CreateCounter(MetricDescriptors.ConnectionsClosed,
				() => ObserveConnectionCounter(c => c.Closed));

			// ── Extended namespace counters ────────────────────────────────
			CreateCounter(MetricDescriptors.NamespaceErrors,
				() => ObserveNamespaceCounter(ns => ns.Errors));

			CreateCounter(MetricDescriptors.NamespaceTimeouts,
				() => ObserveNamespaceCounter(ns => ns.Timeouts));

			CreateCounter(MetricDescriptors.NamespaceKeyBusy,
				() => ObserveNamespaceCounter(ns => ns.KeyBusy));

			CreateCounter(MetricDescriptors.NamespaceBytesIn,
				() => ObserveNamespaceCounter(ns => ns.BytesIn));

			CreateCounter(MetricDescriptors.NamespaceBytesOut,
				() => ObserveNamespaceCounter(ns => ns.BytesOut));

			// ── Extended latency histograms ────────────────────────────────
			CreateCounter(MetricDescriptors.LatencyBucket, ObserveLatencyBuckets);
		}

		private void CreateGauge(MetricDescriptors.Descriptor d, Func<IEnumerable<Measurement<double>>> callback)
		{
			meter.CreateObservableGauge(d.Name, callback, unit: ToOTelUnit(d.Unit), description: d.Description);
		}

		private void CreateCounter(MetricDescriptors.Descriptor d, Func<IEnumerable<Measurement<long>>> callback)
		{
			meter.CreateObservableCounter(d.Name, callback, unit: ToOTelUnit(d.Unit), description: d.Description);
		}

		/// <summary>
		/// Maps plain exporter-agnostic units from <see cref="MetricDescriptors"/>
		/// to OTel/UCUM conventions for instrument registration.
		/// </summary>
		private static string ToOTelUnit(string unit) => unit switch
		{
			"bytes" => "By",
			"%" => "%",
			_ => $"{{{unit}}}"
		};

		#endregion

		#region Standard Observable Callbacks

		private IEnumerable<Measurement<double>> ObserveClusterGauge(Func<MetricsSnapshot, double> selector)
		{
			var snap = latestSnapshot;
			if (snap == null) yield break;
			yield return new Measurement<double>(selector(snap), BuildClusterTags(snap));
		}

		private IEnumerable<Measurement<long>> ObserveClusterCounter(Func<MetricsSnapshot, long> selector)
		{
			var snap = latestSnapshot;
			if (snap == null) yield break;
			yield return new Measurement<long>(selector(snap), BuildClusterTags(snap));
		}

		private IEnumerable<Measurement<double>> ObserveConnectionGauge(Func<ConnectionMetricsSnapshot, int> selector)
		{
			var snap = latestSnapshot;
			if (snap == null) yield break;

			foreach (var node in snap.Nodes)
			{
				var nodeTags = BuildNodeTags(snap, node);
				yield return new Measurement<double>(selector(node.SyncConnections),
					AppendTag(nodeTags, "conn_type", "sync"));
				yield return new Measurement<double>(selector(node.AsyncConnections),
					AppendTag(nodeTags, "conn_type", "async"));
			}
		}

		private IEnumerable<Measurement<long>> ObserveConnectionCounter(Func<ConnectionMetricsSnapshot, int> selector)
		{
			var snap = latestSnapshot;
			if (snap == null) yield break;

			foreach (var node in snap.Nodes)
			{
				var nodeTags = BuildNodeTags(snap, node);
				yield return new Measurement<long>(selector(node.SyncConnections),
					AppendTag(nodeTags, "conn_type", "sync"));
				yield return new Measurement<long>(selector(node.AsyncConnections),
					AppendTag(nodeTags, "conn_type", "async"));
			}
		}

		#endregion

		#region Extended Observable Callbacks

		private IEnumerable<Measurement<double>> ObserveExtendedClusterGauge(Func<MetricsSnapshot, double> selector)
		{
			var snap = latestSnapshot;
			if (snap == null || !snap.ExtendedMetricsEnabled) yield break;
			yield return new Measurement<double>(selector(snap), BuildClusterTags(snap));
		}

		private IEnumerable<Measurement<long>> ObserveExtendedClusterCounter(Func<MetricsSnapshot, long> selector)
		{
			var snap = latestSnapshot;
			if (snap == null || !snap.ExtendedMetricsEnabled) yield break;
			yield return new Measurement<long>(selector(snap), BuildClusterTags(snap));
		}

		private IEnumerable<Measurement<long>> ObserveNamespaceCounter(Func<NamespaceMetricsSnapshot, long> selector)
		{
			var snap = latestSnapshot;
			if (snap == null || !snap.ExtendedMetricsEnabled) yield break;

			foreach (var node in snap.Nodes)
			{
				foreach (var ns in node.Namespaces)
				{
					yield return new Measurement<long>(selector(ns),
						BuildNamespaceTags(snap, node, ns));
				}
			}
		}

		private IEnumerable<Measurement<long>> ObserveLatencyBuckets()
		{
			var snap = latestSnapshot;
			if (snap == null || !snap.ExtendedMetricsEnabled) yield break;

			var bounds = GetBucketBounds(snap.LatencyColumns, snap.LatencyShift);

			foreach (var node in snap.Nodes)
			{
				foreach (var ns in node.Namespaces)
				{
					if (ns.Latencies == null) continue;

					var nsTags = BuildNamespaceTags(snap, node, ns);

					foreach (var lat in ns.Latencies)
					{
						int typeIndex = (int)lat.Type;
						string opName = typeIndex < LatencyTypeNames.Length
							? LatencyTypeNames[typeIndex]
							: lat.Type.ToString().ToLowerInvariant();

						for (int j = 0; j < lat.BucketCounts.Length; j++)
						{
							string bound = j < bounds.Length ? bounds[j] : ComputeLatencyBucketBound(j, snap.LatencyShift);
							yield return new Measurement<long>(lat.BucketCounts[j],
								AppendTag(AppendTag(nsTags, "operation", opName), "le", bound));
						}
					}
				}
			}
		}

		#endregion

		#region Tag Helpers

		private static TagList BuildClusterTags(MetricsSnapshot snap)
		{
			// cluster, client_type, and client_version are resource-level attributes —
			// configure them on the MeterProvider via GetResourceAttributes().
			var tags = new TagList();

			if (snap.AppId != null)
			{
				tags.Add("app_id", snap.AppId);
			}

			if (snap.Labels != null)
			{
				foreach (var kvp in snap.Labels)
				{
					tags.Add(kvp.Key, kvp.Value);
				}
			}

			return tags;
		}

		private static TagList BuildNodeTags(MetricsSnapshot snap, NodeMetricsSnapshot node)
		{
			var tags = BuildClusterTags(snap);
			tags.Add("node", node.NodeName);
			tags.Add("node_address", node.NodeAddress);
			tags.Add("node_port", node.NodePort.ToString());
			return tags;
		}

		private static TagList BuildNamespaceTags(MetricsSnapshot snap, NodeMetricsSnapshot node, NamespaceMetricsSnapshot ns)
		{
			var tags = BuildNodeTags(snap, node);
			tags.Add("namespace", ns.Namespace);
			return tags;
		}

		private static TagList AppendTag(TagList tags, string key, string value)
		{
			tags.Add(key, value);
			return tags;
		}

		private string[] GetBucketBounds(int columns, int shift)
		{
			if (columns == cachedLatencyColumns && shift == cachedLatencyShift)
			{
				return cachedBucketBounds;
			}

			var bounds = new string[columns];
			for (int i = 0; i < columns; i++)
			{
				bounds[i] = ComputeLatencyBucketBound(i, shift);
			}

			cachedBucketBounds = bounds;
			cachedLatencyColumns = columns;
			cachedLatencyShift = shift;
			return bounds;
		}

		private static string ComputeLatencyBucketBound(int bucketIndex, int latencyShift)
		{
			if (bucketIndex == 0) return "1ms";
			int boundMs = 1 << (bucketIndex * latencyShift);
			return boundMs >= 1000 ? $"{boundMs / 1000}s" : $"{boundMs}ms";
		}

		#endregion
	}
}
