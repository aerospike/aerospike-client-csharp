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
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	internal sealed partial class MetricsStore
	{
		internal void RemoveNode(Node node)
		{
			BeginNodeDeparture(node);
		}

		internal MetricsNodeDeparture BeginNodeDeparture(Node node)
		{
			return BeginNodeDeparture(node, null);
		}

		internal MetricsNodeDeparture BeginNodeDeparture(
			Node node,
			Action collectorResolved)
		{
			MetricsNodeDeparture current = node.MetricsDeparture;
			if (current != null &&
				ReferenceEquals(current.Store, this))
			{
				return current;
			}

			MetricsNodeGeneration generation = RegisterNode(node);
			collectorResolved?.Invoke();
			current = new MetricsNodeDeparture(this, generation.Collector);
			node.InstallMetricsDeparture(current);
			nodeNames[generation.Name].Depart(generation);
			return current;
		}

		internal void RecordNodeDeparture(
			Node node,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			RecordNodeDeparture(
				generation.Name,
				node.address.Address.ToString(),
				node.address.Port,
				node.GetErrorRate(),
				nodeNames[generation.Name],
				compatibility);
		}

		internal void RecordNodeDeparture(
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeNameState state,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			lock (departureLock)
			{
				if (!departures.TryGetValue(
					name,
					out DepartureEntry entry))
				{
					if (departures.Count >= MaximumDepartureNames)
					{
						DepartureEntry reusable = departures.Values
							.Where(candidate =>
								candidate.Acknowledged != null &&
								!candidate.State.IsActive &&
								MetricsNodeNameState.IsZero(
									Subtract(
										candidate.State.Read(
											candidate.Name,
											config),
										candidate.Acknowledged)))
							.OrderBy(candidate => candidate.Generation)
							.FirstOrDefault();

						if (reusable == null)
						{
							departureTruncationGeneration++;
							return;
						}
						departures.Remove(reusable.Name);
					}

					entry = new DepartureEntry(name, state);
					departures.Add(name, entry);
				}

				entry.Address = address ?? string.Empty;
				entry.Port = port;
				entry.ErrorRate = errorRate;
				entry.Generation = ++departureGeneration;
				entry.Acknowledged = null;
				entry.Compatibility = MergeCompatibilityNodes(
					entry.Compatibility,
					compatibility,
					config.Shape);
			}
		}

		internal MetricsDepartureCapture CaptureDepartures(
			MetricsRuntimeConfig expected)
		{
			return CaptureDepartures(expected, null);
		}

		private MetricsDepartureCapture CaptureDepartures(
			MetricsRuntimeConfig expected,
			IReadOnlyDictionary<string, MetricsNodeCapture> captures)
		{
			if (expected == null)
			{
				return MetricsDepartureCapture.Empty;
			}

			lock (departureLock)
			{
				List<MetricsDepartedNodeCapture> nodes =
					new(departures.Count);
				List<MetricsDepartureWatermark> watermarks = new();

				foreach (DepartureEntry entry in departures.Values.OrderBy(
					entry => entry.Name,
					StringComparer.Ordinal))
				{
					MetricsNodeCapture capture =
						captures != null &&
						captures.TryGetValue(
							entry.Name,
							out MetricsNodeCapture saved)
							? saved
							: entry.State.Capture(entry.Name, expected);
					MetricsNodeView current = capture.Public;
					MetricsNodeView delta = entry.Acknowledged == null
						? current
						: Subtract(current, entry.Acknowledged);

					if (!capture.Active &&
						(entry.Acknowledged == null ||
							!MetricsNodeNameState.IsZero(delta)))
					{
						nodes.Add(new MetricsDepartedNodeCapture(
							entry.Name,
							entry.Address,
							entry.Port,
							entry.ErrorRate,
							delta,
							BuildCompatibilitySnapshot(
								entry,
								capture.Compatibility,
								expected.Shape)));
					}

					watermarks.Add(new MetricsDepartureWatermark(
						entry.Name,
						entry.Generation,
						current,
						expected.Shape));
				}

				MetricsDepartureToken token = new(
					this,
					watermarks,
					departureTruncationGeneration);
				return new MetricsDepartureCapture(
					nodes,
					departureTruncationGeneration != 0,
					token);
			}
		}

		internal void AcknowledgeDepartures(MetricsDepartureToken token)
		{
			if (token == null || !ReferenceEquals(token.Store, this))
			{
				return;
			}

			lock (departureLock)
			{
				foreach (MetricsDepartureWatermark watermark in token.Watermarks)
				{
					if (!departures.TryGetValue(
						watermark.Name,
						out DepartureEntry entry) ||
						entry.Generation != watermark.Generation)
					{
						continue;
					}

					entry.Acknowledged = watermark.Captured;
				}

				if (token.TruncationGeneration != 0 &&
					token.TruncationGeneration ==
						departureTruncationGeneration)
				{
					departureTruncationGeneration = 0;
				}
			}
		}

		private static MetricsNodeView Subtract(
			MetricsNodeView current,
			MetricsNodeView baseline)
		{
			Dictionary<string, MetricsNamespaceView> namespaces =
				new(StringComparer.Ordinal);
			HashSet<string> names = new(
				current.Namespaces.Keys,
				StringComparer.Ordinal);
			names.UnionWith(baseline.Namespaces.Keys);

			foreach (string name in names)
			{
				current.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView currentNamespace);
				baseline.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView baselineNamespace);
				namespaces[name] = SubtractNamespace(
					name,
					currentNamespace,
					baselineNamespace);
			}

			return new MetricsNodeView(
				current.Name,
				Subtract(current.Opened, baseline.Opened),
				Subtract(current.Closed, baseline.Closed),
				Subtract(current.PoolExhausted, baseline.PoolExhausted),
				Subtract(current.OpenFailures, baseline.OpenFailures),
				Subtract(
					current.TlsHandshakeFailures,
					baseline.TlsHandshakeFailures),
				Subtract(current.AuthFailures, baseline.AuthFailures),
				SubtractBuckets(
					current.ConnectionLatency,
					baseline.ConnectionLatency),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					namespaces));
		}

		private static MetricsNamespaceView SubtractNamespace(
			string name,
			MetricsNamespaceView current,
			MetricsNamespaceView baseline)
		{
			Dictionary<LatencyType, IReadOnlyList<long>> latency = new();

			for (int i = 0; i < Latency.GetMax(); i++)
			{
				LatencyType type = (LatencyType)i;
				IReadOnlyList<long> currentBuckets =
					GetBuckets(current, type);
				IReadOnlyList<long> baselineBuckets =
					GetBuckets(baseline, type);
				int count = Math.Max(
					currentBuckets.Count,
					baselineBuckets.Count);
				long[] delta = new long[count];

				for (int j = 0; j < count; j++)
				{
					long currentValue =
						j < currentBuckets.Count ? currentBuckets[j] : 0;
					long baselineValue =
						j < baselineBuckets.Count ? baselineBuckets[j] : 0;
					delta[j] = Subtract(currentValue, baselineValue);
				}
				latency[type] = Array.AsReadOnly(delta);
			}

			return new MetricsNamespaceView(
				name,
				Subtract(current?.Errors ?? 0, baseline?.Errors ?? 0),
				Subtract(current?.Timeouts ?? 0, baseline?.Timeouts ?? 0),
				Subtract(
					current?.ClientTimeouts ?? 0,
					baseline?.ClientTimeouts ?? 0),
				Subtract(
					current?.ServerTimeouts ?? 0,
					baseline?.ServerTimeouts ?? 0),
				Subtract(current?.KeyBusy ?? 0, baseline?.KeyBusy ?? 0),
				Subtract(
					current?.RecordTooBig ?? 0,
					baseline?.RecordTooBig ?? 0),
				Subtract(
					current?.DeviceOverload ?? 0,
					baseline?.DeviceOverload ?? 0),
				Subtract(current?.BytesIn ?? 0, baseline?.BytesIn ?? 0),
				Subtract(current?.BytesOut ?? 0, baseline?.BytesOut ?? 0),
				new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(
					latency));
		}

		private static IReadOnlyList<long> GetBuckets(
			MetricsNamespaceView view,
			LatencyType type)
		{
			return view != null &&
				view.Latency.TryGetValue(
					type,
					out IReadOnlyList<long> buckets)
					? buckets
					: Array.Empty<long>();
		}

		private static long Subtract(long value, long baseline)
		{
			return value >= baseline ? value - baseline : value;
		}

		private static IReadOnlyList<long> SubtractBuckets(
			IReadOnlyList<long> value,
			IReadOnlyList<long> baseline)
		{
			int count = Math.Max(value.Count, baseline.Count);
			long[] result = new long[count];

			for (int i = 0; i < count; i++)
			{
				long current = i < value.Count ? value[i] : 0;
				long previous = i < baseline.Count ? baseline[i] : 0;
				result[i] = Subtract(current, previous);
			}
			return Array.AsReadOnly(result);
		}

		private static MetricsNodeCompatibilitySnapshot
			BuildCompatibilitySnapshot(
				DepartureEntry entry,
				MetricsNodeView view,
				MetricsHistogramShape shape)
		{
			MetricsNodeCompatibilitySnapshot template =
				NormalizeCompatibility(entry.Compatibility, shape);

			if (template == null)
			{
				return null;
			}

			int asyncOpened = template.AsyncConnections.opened;
			int asyncClosed = template.AsyncConnections.closed;
			ConnectionStats sync = new(
				template.SyncConnections.inPool,
				template.SyncConnections.inUse,
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Opened - asyncOpened)),
				(int)Math.Min(
					int.MaxValue,
					Math.Max(0, view.Closed - asyncClosed)));
			List<MetricsNamespaceCompatibilitySnapshot> namespaces =
				new();
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot>
				templateNamespaces = template.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			HashSet<string> names = new(
				view.Namespaces.Keys,
				StringComparer.Ordinal);
			names.UnionWith(templateNamespaces.Keys);

			foreach (string name in names.OrderBy(
				name => name,
				StringComparer.Ordinal))
			{
				view.Namespaces.TryGetValue(
					name,
					out MetricsNamespaceView ns);
				templateNamespaces.TryGetValue(
					name,
					out MetricsNamespaceCompatibilitySnapshot saved);
				namespaces.Add(new MetricsNamespaceCompatibilitySnapshot(
					name,
					Math.Max(ns?.Errors ?? 0, saved?.Errors ?? 0),
					Math.Max(ns?.Timeouts ?? 0, saved?.Timeouts ?? 0),
					Math.Max(ns?.KeyBusy ?? 0, saved?.KeyBusy ?? 0),
					Math.Max(ns?.BytesIn ?? 0, saved?.BytesIn ?? 0),
					Math.Max(ns?.BytesOut ?? 0, saved?.BytesOut ?? 0),
					Enumerable.Range(0, Latency.GetMax()).Select(index =>
					{
						IReadOnlyList<long> current =
							ns != null &&
							ns.Latency.TryGetValue(
								(LatencyType)index,
								out IReadOnlyList<long> buckets)
								? buckets
								: Array.Empty<long>();
						IReadOnlyList<long> previous =
							saved != null && index < saved.Latency.Count
								? saved.Latency[index]
								: Array.Empty<long>();
						return (IReadOnlyList<long>)Array.AsReadOnly(
							Enumerable.Range(0, shape.Columns)
								.Select(bucket => Math.Max(
									bucket < current.Count
										? current[bucket]
										: 0,
									bucket < previous.Count
										? previous[bucket]
										: 0))
								.ToArray());
					})));
			}

			return new MetricsNodeCompatibilitySnapshot(
				entry.Name,
				entry.Address,
				entry.Port,
				sync,
				template.AsyncConnections,
				namespaces,
				shape.Unit,
				shape.Columns,
				shape.Shift);
		}

		private static MetricsNodeCompatibilitySnapshot
			MergeCompatibilityNodes(
				MetricsNodeCompatibilitySnapshot previous,
				MetricsNodeCompatibilitySnapshot current,
				MetricsHistogramShape shape)
		{
			if (previous == null)
			{
				return NormalizeCompatibility(current, shape);
			}
			if (current == null)
			{
				return NormalizeCompatibility(previous, shape);
			}

			previous = NormalizeCompatibility(previous, shape);
			current = NormalizeCompatibility(current, shape);
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot> left =
				previous.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			Dictionary<string, MetricsNamespaceCompatibilitySnapshot> right =
				current.Namespaces.ToDictionary(
					ns => ns.Name,
					StringComparer.Ordinal);
			HashSet<string> names = new(left.Keys, StringComparer.Ordinal);
			names.UnionWith(right.Keys);

			IEnumerable<MetricsNamespaceCompatibilitySnapshot> namespaces =
				names.OrderBy(name => name, StringComparer.Ordinal).Select(name =>
				{
					left.TryGetValue(
						name,
						out MetricsNamespaceCompatibilitySnapshot first);
					right.TryGetValue(
						name,
						out MetricsNamespaceCompatibilitySnapshot second);
					return new MetricsNamespaceCompatibilitySnapshot(
						name,
						(first?.Errors ?? 0) + (second?.Errors ?? 0),
						(first?.Timeouts ?? 0) + (second?.Timeouts ?? 0),
						(first?.KeyBusy ?? 0) + (second?.KeyBusy ?? 0),
						(first?.BytesIn ?? 0) + (second?.BytesIn ?? 0),
						(first?.BytesOut ?? 0) + (second?.BytesOut ?? 0),
						Enumerable.Range(0, Latency.GetMax()).Select(index =>
							(IReadOnlyList<long>)Array.AsReadOnly(
								Enumerable.Range(0, shape.Columns)
									.Select(bucket =>
										(first != null
											? first.Latency[index][bucket]
											: 0) +
										(second != null
											? second.Latency[index][bucket]
											: 0))
									.ToArray())));
				});

			return new MetricsNodeCompatibilitySnapshot(
				current.Name,
				current.Address,
				current.Port,
				new ConnectionStats(
					current.SyncConnections.inPool,
					current.SyncConnections.inUse,
					AddInt(
						previous.SyncConnections.opened,
						current.SyncConnections.opened),
					AddInt(
						previous.SyncConnections.closed,
						current.SyncConnections.closed)),
				new ConnectionStats(
					current.AsyncConnections.inPool,
					current.AsyncConnections.inUse,
					AddInt(
						previous.AsyncConnections.opened,
						current.AsyncConnections.opened),
					AddInt(
						previous.AsyncConnections.closed,
						current.AsyncConnections.closed)),
				namespaces,
				shape.Unit,
				shape.Columns,
				shape.Shift);
		}

		private static int AddInt(int left, int right)
		{
			return (int)Math.Min(int.MaxValue, (long)left + right);
		}

		private static MetricsNodeCompatibilitySnapshot NormalizeCompatibility(
			MetricsNodeCompatibilitySnapshot node,
			MetricsHistogramShape shape)
		{
			if (node == null ||
				(node.LatencyUnit == shape.Unit &&
					node.LatencyColumns == shape.Columns &&
					node.LatencyShift == shape.Shift))
			{
				return node;
			}

			return new MetricsNodeCompatibilitySnapshot(
				node.Name,
				node.Address,
				node.Port,
				node.SyncConnections,
				node.AsyncConnections,
				node.Namespaces.Select(ns =>
					new MetricsNamespaceCompatibilitySnapshot(
						ns.Name,
						ns.Errors,
						ns.Timeouts,
						ns.KeyBusy,
						ns.BytesIn,
						ns.BytesOut,
						Enumerable.Range(
							0,
							Latency.GetMax()).Select(_ =>
								(IReadOnlyList<long>)Array.AsReadOnly(
									new long[shape.Columns])))),
				shape.Unit,
				shape.Columns,
				shape.Shift);
		}
	}
}
