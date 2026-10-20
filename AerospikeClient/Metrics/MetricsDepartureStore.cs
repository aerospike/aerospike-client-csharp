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
	internal sealed class MetricsDepartureStore
	{
		private const int MaximumNames = 256;

		private sealed class Entry
		{
			internal string Name;
			internal string Address;
			internal int Port;
			internal int ErrorRate;
			internal readonly List<MetricsDepartureContribution> Contributions = new();
		}

		private readonly object sync = new();
		private readonly Dictionary<string, Entry> entries =
			new(StringComparer.Ordinal);
		private long truncationGeneration;
		private long contributionGeneration;

		internal void Add(
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeDeparture departure,
			LegacyMetricsFileNodeSnapshot legacy)
		{
			if (departure == null)
			{
				return;
			}

			name ??= string.Empty;
			lock (sync)
			{
				if (!entries.TryGetValue(name, out Entry entry))
				{
					if (entries.Count >= MaximumNames)
					{
						truncationGeneration++;
						return;
					}

					entry = new Entry
					{
						Name = name,
						Address = address ?? string.Empty,
						Port = port,
						ErrorRate = errorRate
					};
					entries.Add(name, entry);
				}

				entry.Name = name;
				entry.Address = address ?? string.Empty;
				entry.Port = port;
				entry.ErrorRate = errorRate;

				MetricsDepartureContribution contribution;
				if (entry.Contributions.Count == 0)
				{
					MetricsNodeCollector aggregate =
						new(departure.Collector.CurrentShape);
					contribution = new MetricsDepartureContribution(
						++contributionGeneration,
						entry.Name,
						entry.Address,
						entry.Port,
						entry.ErrorRate,
						aggregate,
						null)
					{
						Active = true
					};
					entry.Contributions.Add(contribution);
					aggregate.SetChangedObserver(
						() => Reactivate(contribution));
				}
				else
				{
					contribution = entry.Contributions[0];
				}
				contribution.UpdateIdentity(
					entry.Address,
					entry.Port,
					entry.ErrorRate);
				contribution.Collector.EnsureShape(
					departure.Collector.CurrentShape);
				contribution.Legacy = MergeLegacyNodes(
					[contribution.Legacy, legacy],
					contribution.Collector.CurrentShape);
				new MetricsDepartureForwarder(
					departure.Collector,
					contribution.Collector,
					entry.Name).Attach();
			}
		}

		private void Reactivate(MetricsDepartureContribution contribution)
		{
			lock (sync)
			{
				if (contribution.Active)
				{
					return;
				}

				if (!entries.TryGetValue(contribution.Name, out Entry entry))
				{
					if (entries.Count >= MaximumNames)
					{
						truncationGeneration++;
						return;
					}

					entry = new Entry
					{
						Name = contribution.Name,
						Address = contribution.Address,
						Port = contribution.Port,
						ErrorRate = contribution.ErrorRate
					};
					entry.Contributions.Add(contribution);
					entries.Add(entry.Name, entry);
					contribution.Active = true;
					return;
				}

				MetricsDepartureContribution target =
					entry.Contributions[0];
				MetricsHistogramShape targetShape =
					target.Collector.CurrentShape;
				MetricsNodeView current = contribution.Collector.Read(
					contribution.Name,
					contribution.Collector.CurrentShape);
				MetricsNodeView delta = contribution.Acknowledged == null
					? current
					: Subtract(current, contribution.Acknowledged);
				if (!IsZero(delta))
				{
					target.Collector.Merge(
						NormalizeShape(
							delta,
							contribution.Collector.CurrentShape,
							targetShape),
						targetShape);
				}
				contribution.Acknowledged = current;
				if (contribution.LastLegacyTargetGeneration !=
					target.Generation)
				{
					target.Legacy = MergeLegacyNodes(
						[target.Legacy, contribution.Legacy],
						targetShape);
					contribution.LastLegacyTargetGeneration =
						target.Generation;
				}
			}
		}

		internal MetricsDepartureCapture Capture(MetricsRuntimeConfig expected)
		{
			if (expected == null)
			{
				return MetricsDepartureCapture.Empty;
			}

			lock (sync)
			{
				List<MetricsDepartedNodeCapture> nodes = new(entries.Count);
				List<MetricsDepartureWatermark> watermarks = new();

				foreach (Entry entry in entries.Values.OrderBy(
					entry => entry.Name,
					StringComparer.Ordinal))
				{
					MetricsNodeCollector aggregate = new(expected);
					bool include = false;

					foreach (MetricsDepartureContribution contribution in
						entry.Contributions)
					{
						MetricsHistogramShape sourceShape =
							contribution.Collector.CurrentShape;
						MetricsNodeView current =
							contribution.Collector.Read(
								entry.Name,
								sourceShape);
						MetricsNodeView delta = contribution.Acknowledged == null
							? current
							: Subtract(current, contribution.Acknowledged);

						if (contribution.Acknowledged == null || !IsZero(delta))
						{
							aggregate.Merge(
								NormalizeShape(
									delta,
									sourceShape,
									expected.Shape),
								expected);
							include = true;
						}
						watermarks.Add(new MetricsDepartureWatermark(
							contribution,
							current,
							sourceShape));
					}

					if (include)
					{
						nodes.Add(new MetricsDepartedNodeCapture(
							entry.Name,
							entry.Address,
							entry.Port,
							entry.ErrorRate,
							aggregate.Read(entry.Name, expected),
							MergeLegacy(entry, expected.Shape)));
					}
				}

				MetricsDepartureToken token = new(
					watermarks,
					truncationGeneration);
				return new MetricsDepartureCapture(
					nodes,
					truncationGeneration != 0,
					token);
			}
		}

		internal void Acknowledge(MetricsDepartureToken token)
		{
			if (token == null)
			{
				return;
			}

			lock (sync)
			{
				foreach (MetricsDepartureWatermark watermark in token.Watermarks)
				{
					MetricsDepartureContribution contribution =
						watermark.Contribution;

					if (!entries.TryGetValue(
						contribution.Name,
						out Entry entry) ||
						!entry.Contributions.Contains(contribution))
					{
						continue;
					}

					MetricsNodeView current = contribution.Collector.Read(
						contribution.Name,
						watermark.SourceShape);

					if (Equal(current, watermark.Captured))
					{
						contribution.Acknowledged = watermark.Captured;
						entry.Contributions.Remove(contribution);
						contribution.Active = false;
					}
					else
					{
						contribution.Acknowledged = watermark.Captured;
					}

					if (entry.Contributions.Count == 0)
					{
						entries.Remove(entry.Name);
					}
				}

				if (token.TruncationGeneration != 0 &&
					token.TruncationGeneration == truncationGeneration)
				{
					truncationGeneration = 0;
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
				current.Namespaces.TryGetValue(name, out MetricsNamespaceView currentNs);
				baseline.Namespaces.TryGetValue(name, out MetricsNamespaceView baselineNs);
				namespaces[name] = SubtractNamespace(
					name,
					currentNs,
					baselineNs);
			}

			return new MetricsNodeView(
				current.Name,
				Subtract(current.Opened, baseline.Opened),
				Subtract(current.Closed, baseline.Closed),
				Subtract(current.PoolExhausted, baseline.PoolExhausted),
				Subtract(current.ClosedError, baseline.ClosedError),
				Subtract(current.ClosedIdle, baseline.ClosedIdle),
				Subtract(current.ClosedPoolShrink, baseline.ClosedPoolShrink),
				Subtract(current.ClosedNodeRemoved, baseline.ClosedNodeRemoved),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(namespaces));
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
				int count = Math.Max(currentBuckets.Count, baselineBuckets.Count);
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
				Subtract(current?.KeyBusy ?? 0, baseline?.KeyBusy ?? 0),
				Subtract(current?.RecordTooBig ?? 0, baseline?.RecordTooBig ?? 0),
				Subtract(current?.DeviceOverload ?? 0, baseline?.DeviceOverload ?? 0),
				Subtract(current?.BytesIn ?? 0, baseline?.BytesIn ?? 0),
				Subtract(current?.BytesOut ?? 0, baseline?.BytesOut ?? 0),
				new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(latency));
		}

		private static IReadOnlyList<long> GetBuckets(
			MetricsNamespaceView view,
			LatencyType type)
		{
			return view != null &&
				view.Latency.TryGetValue(type, out IReadOnlyList<long> buckets)
					? buckets
					: Array.Empty<long>();
		}

		private static bool IsZero(MetricsNodeView view)
		{
			if (view.Opened != 0 ||
				view.Closed != 0 ||
				view.PoolExhausted != 0 ||
				view.ClosedError != 0 ||
				view.ClosedIdle != 0 ||
				view.ClosedPoolShrink != 0 ||
				view.ClosedNodeRemoved != 0)
			{
				return false;
			}

			foreach (MetricsNamespaceView ns in view.Namespaces.Values)
			{
				if (ns.Errors != 0 ||
					ns.Timeouts != 0 ||
					ns.KeyBusy != 0 ||
					ns.RecordTooBig != 0 ||
					ns.DeviceOverload != 0 ||
					ns.BytesIn != 0 ||
					ns.BytesOut != 0 ||
					ns.Latency.Values.Any(buckets => buckets.Any(value => value != 0)))
				{
					return false;
				}
			}
			return true;
		}

		private static bool Equal(MetricsNodeView left, MetricsNodeView right)
		{
			if (left.Opened != right.Opened ||
				left.Closed != right.Closed ||
				left.PoolExhausted != right.PoolExhausted ||
				left.ClosedError != right.ClosedError ||
				left.ClosedIdle != right.ClosedIdle ||
				left.ClosedPoolShrink != right.ClosedPoolShrink ||
				left.ClosedNodeRemoved != right.ClosedNodeRemoved ||
				left.Namespaces.Count != right.Namespaces.Count)
			{
				return false;
			}

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in left.Namespaces)
			{
				if (!right.Namespaces.TryGetValue(
					entry.Key,
					out MetricsNamespaceView rightNs) ||
					!Equal(entry.Value, rightNs))
				{
					return false;
				}
			}
			return true;
		}

		private static bool Equal(
			MetricsNamespaceView left,
			MetricsNamespaceView right)
		{
			if (left.Errors != right.Errors ||
				left.Timeouts != right.Timeouts ||
				left.KeyBusy != right.KeyBusy ||
				left.RecordTooBig != right.RecordTooBig ||
				left.DeviceOverload != right.DeviceOverload ||
				left.BytesIn != right.BytesIn ||
				left.BytesOut != right.BytesOut ||
				left.Latency.Count != right.Latency.Count)
			{
				return false;
			}

			foreach (KeyValuePair<LatencyType, IReadOnlyList<long>> entry in left.Latency)
			{
				if (!right.Latency.TryGetValue(
					entry.Key,
					out IReadOnlyList<long> rightBuckets) ||
					!entry.Value.SequenceEqual(rightBuckets))
				{
					return false;
				}
			}
			return true;
		}

		private static long Subtract(long value, long baseline)
		{
			return value >= baseline ? value - baseline : value;
		}

		private static MetricsNodeView NormalizeShape(
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
					ns.KeyBusy,
					ns.RecordTooBig,
					ns.DeviceOverload,
					ns.BytesIn,
					ns.BytesOut,
					new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(
						latency));
			}

			return new MetricsNodeView(
				source.Name,
				source.Opened,
				source.Closed,
				source.PoolExhausted,
				source.ClosedError,
				source.ClosedIdle,
				source.ClosedPoolShrink,
				source.ClosedNodeRemoved,
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					namespaces));
		}

		private static LegacyMetricsFileNodeSnapshot MergeLegacy(
			Entry entry,
			MetricsHistogramShape shape)
		{
			return MergeLegacyNodes(
				entry.Contributions.Select(contribution => contribution.Legacy),
				shape);
		}

		private static LegacyMetricsFileNodeSnapshot MergeLegacyNodes(
			IEnumerable<LegacyMetricsFileNodeSnapshot> source,
			MetricsHistogramShape shape)
		{
			List<LegacyMetricsFileNodeSnapshot> nodes =
				source.Where(node => node != null).ToList();

			if (nodes.Count == 0)
			{
				return null;
			}

			LegacyMetricsFileNodeSnapshot newest =
				nodes[^1];

			if (nodes.Count == 1)
			{
				return NormalizeLegacy(newest, shape);
			}

			long syncOpened = 0;
			long syncClosed = 0;
			long asyncOpened = 0;
			long asyncClosed = 0;
			Dictionary<string, LegacyNamespaceAccumulator> namespaces =
				new(StringComparer.Ordinal);

			foreach (LegacyMetricsFileNodeSnapshot node in nodes)
			{
				syncOpened += node.SyncConnections.opened;
				syncClosed += node.SyncConnections.closed;
				asyncOpened += node.AsyncConnections.opened;
				asyncClosed += node.AsyncConnections.closed;

				foreach (LegacyMetricsFileNamespaceSnapshot ns in node.Namespaces)
				{
					if (!namespaces.TryGetValue(
						ns.Name,
						out LegacyNamespaceAccumulator accumulator))
					{
						accumulator =
							new LegacyNamespaceAccumulator(shape.Columns);
						namespaces.Add(ns.Name, accumulator);
					}
					accumulator.Add(
						ns,
						node.LatencyUnit == shape.Unit &&
							node.LatencyColumns == shape.Columns &&
							node.LatencyShift == shape.Shift);
				}
			}

			ConnectionStats syncConnections = new(
				newest.SyncConnections.inPool,
				newest.SyncConnections.inUse,
				(int)Math.Min(int.MaxValue, syncOpened),
				(int)Math.Min(int.MaxValue, syncClosed));
			ConnectionStats asyncConnections = new(
				newest.AsyncConnections.inPool,
				newest.AsyncConnections.inUse,
				(int)Math.Min(int.MaxValue, asyncOpened),
				(int)Math.Min(int.MaxValue, asyncClosed));
			return NormalizeLegacy(new LegacyMetricsFileNodeSnapshot(
				newest.Name,
				newest.Address,
				newest.Port,
				syncConnections,
				asyncConnections,
				namespaces.OrderBy(
					entry => entry.Key,
					StringComparer.Ordinal).Select(entry => entry.Value.Build(entry.Key)),
				newest.LatencyUnit,
				newest.LatencyColumns,
				newest.LatencyShift),
				shape);
		}

		private static LegacyMetricsFileNodeSnapshot NormalizeLegacy(
			LegacyMetricsFileNodeSnapshot node,
			MetricsHistogramShape shape)
		{
			if (node.LatencyUnit == shape.Unit &&
				node.LatencyColumns == shape.Columns &&
				node.LatencyShift == shape.Shift)
			{
				return node;
			}

			return new LegacyMetricsFileNodeSnapshot(
				node.Name,
				node.Address,
				node.Port,
				node.SyncConnections,
				node.AsyncConnections,
				node.Namespaces.Select(ns =>
					new LegacyMetricsFileNamespaceSnapshot(
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

		private sealed class LegacyNamespaceAccumulator
		{
			private long errors;
			private long timeouts;
			private long keyBusy;
			private long bytesIn;
			private long bytesOut;
			private readonly List<long[]> latency;

			internal LegacyNamespaceAccumulator(int columns)
			{
				latency = Enumerable.Range(0, Latency.GetMax())
					.Select(_ => new long[columns])
					.ToList();
			}

			internal void Add(
				LegacyMetricsFileNamespaceSnapshot ns,
				bool includeLatency)
			{
				errors += ns.Errors;
				timeouts += ns.Timeouts;
				keyBusy += ns.KeyBusy;
				bytesIn += ns.BytesIn;
				bytesOut += ns.BytesOut;

				if (!includeLatency)
				{
					return;
				}

				for (int i = 0; i < Math.Min(latency.Count, ns.Latency.Count); i++)
				{
					for (int j = 0; j < Math.Min(
						latency[i].Length,
						ns.Latency[i].Count); j++)
					{
						latency[i][j] += ns.Latency[i][j];
					}
				}
			}

			internal LegacyMetricsFileNamespaceSnapshot Build(string name)
			{
				return new LegacyMetricsFileNamespaceSnapshot(
					name,
					errors,
					timeouts,
					keyBusy,
					bytesIn,
					bytesOut,
					latency.Select(buckets => (IReadOnlyList<long>)
						Array.AsReadOnly(buckets)));
			}
		}

		private sealed class MetricsDepartureForwarder
		{
			private readonly object sync = new();
			private readonly MetricsNodeCollector source;
			private readonly MetricsNodeCollector target;
			private readonly string name;
			private MetricsNodeView baseline;

			internal MetricsDepartureForwarder(
				MetricsNodeCollector source,
				MetricsNodeCollector target,
				string name)
			{
				this.source = source;
				this.target = target;
				this.name = name;
			}

			internal void Attach()
			{
				source.SetChangedObserver(Capture);
				Capture();
			}

			private void Capture()
			{
				lock (sync)
				{
					MetricsHistogramShape sourceShape =
						source.CurrentShape;
					MetricsNodeView current =
						source.Read(name, sourceShape);
					MetricsNodeView delta = baseline == null
						? current
						: Subtract(current, baseline);
					baseline = current;

					if (!IsZero(delta))
					{
						MetricsHistogramShape targetShape =
							target.CurrentShape;
						target.Merge(
							NormalizeShape(
								delta,
								sourceShape,
								targetShape),
							targetShape);
					}
				}
			}
		}
	}

	internal sealed class MetricsDepartureContribution
	{
		internal readonly long Generation;
		internal readonly string Name;
		internal string Address;
		internal int Port;
		internal int ErrorRate;
		internal readonly MetricsNodeCollector Collector;
		internal LegacyMetricsFileNodeSnapshot Legacy;
		internal MetricsNodeView Acknowledged;
		internal bool Active;
		internal long LastLegacyTargetGeneration;

		internal MetricsDepartureContribution(
			long generation,
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeCollector collector,
			LegacyMetricsFileNodeSnapshot legacy)
		{
			Generation = generation;
			Name = name;
			Address = address;
			Port = port;
			ErrorRate = errorRate;
			Collector = collector;
			Legacy = legacy;
		}

		internal void UpdateIdentity(
			string address,
			int port,
			int errorRate)
		{
			Address = address;
			Port = port;
			ErrorRate = errorRate;
		}
	}

	internal sealed class MetricsDepartureWatermark
	{
		internal readonly MetricsDepartureContribution Contribution;
		internal readonly MetricsNodeView Captured;
		internal readonly MetricsHistogramShape SourceShape;

		internal MetricsDepartureWatermark(
			MetricsDepartureContribution contribution,
			MetricsNodeView captured,
			MetricsHistogramShape sourceShape)
		{
			Contribution = contribution;
			Captured = captured;
			SourceShape = sourceShape;
		}
	}

	internal sealed class MetricsDepartureToken
	{
		internal readonly IReadOnlyList<MetricsDepartureWatermark> Watermarks;
		internal readonly long TruncationGeneration;

		internal MetricsDepartureToken(
			IEnumerable<MetricsDepartureWatermark> watermarks,
			long truncationGeneration)
		{
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
		internal readonly LegacyMetricsFileNodeSnapshot Legacy;

		internal MetricsDepartedNodeCapture(
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeView view,
			LegacyMetricsFileNodeSnapshot legacy)
		{
			Name = name;
			Address = address;
			Port = port;
			ErrorRate = errorRate;
			View = view;
			Legacy = legacy;
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
