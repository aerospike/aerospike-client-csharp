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
	internal sealed class MetricsHistogramShape
	{
		internal readonly long Generation;
		internal readonly MetricsLatencyUnit Unit;
		internal readonly int Columns;
		internal readonly int Shift;
		internal readonly ulong[] Limits;

		internal MetricsHistogramShape(
			long generation,
			MetricsLatencyUnit unit,
			int columns,
			int shift)
		{
			Generation = generation;
			Unit = unit;
			Columns = columns;
			Shift = shift;
			Limits = CreateLimits(columns, shift);
		}

		internal bool Matches(MetricsPolicy policy)
		{
			return Unit == policy.LatencyUnit &&
				Columns == policy.LatencyColumns &&
				Shift == policy.LatencyShift;
		}

		internal bool Matches(MetricsHistogramShape other)
		{
			return other != null &&
				Unit == other.Unit &&
				Columns == other.Columns &&
				Shift == other.Shift;
		}

		internal bool ShouldReplaceWith(MetricsHistogramShape replacement)
		{
			return replacement.Generation > Generation ||
				(replacement.Generation == Generation &&
					!Matches(replacement));
		}

		/// <summary>
		/// True when buckets recorded under this shape are valid buckets for
		/// <paramref name="expected"/>: same boundaries and not superseded by it.
		/// </summary>
		internal bool CanBeReadAs(MetricsHistogramShape expected)
		{
			return Matches(expected) && Generation >= expected.Generation;
		}

		private static ulong[] CreateLimits(int columns, int shift)
		{
			ulong[] result = new ulong[columns];
			ulong limit = 1;

			for (int i = 0; i < columns; i++)
			{
				result[i] = limit;

				if (shift >= 64 || limit > (ulong.MaxValue >> shift))
				{
					limit = ulong.MaxValue;
				}
				else
				{
					limit <<= shift;
				}
			}
			return result;
		}
	}

	internal sealed class MetricsHistogram
	{
		private readonly long[] buckets;
		private readonly ulong[] limits;
		private readonly MetricsLatencyUnit unit;

		internal MetricsHistogram(MetricsHistogramShape shape)
		{
			buckets = new long[shape.Columns];
			limits = shape.Limits;
			unit = shape.Unit;
		}

		internal void AddNanoseconds(long elapsedNanoseconds)
		{
			ulong divisor = unit == MetricsLatencyUnit.Microseconds ? 1_000UL : 1_000_000UL;
			ulong value = elapsedNanoseconds <= 0 ? 0 : (ulong)elapsedNanoseconds / divisor;
			int index = limits.Length - 1;

			for (int i = 0; i < limits.Length - 1; i++)
			{
				if (value <= limits[i])
				{
					index = i;
					break;
				}
			}
			Interlocked.Increment(ref buckets[index]);
		}

		internal long[] CopyBuckets()
		{
			long[] copy = new long[buckets.Length];

			for (int i = 0; i < buckets.Length; i++)
			{
				copy[i] = Volatile.Read(ref buckets[i]);
			}
			return copy;
		}

		internal void AddBuckets(IReadOnlyList<long> source)
		{
			int count = Math.Min(buckets.Length, source.Count);

			for (int i = 0; i < count; i++)
			{
				Interlocked.Add(ref buckets[i], source[i]);
			}
		}

	}

	internal sealed class MetricsNamespaceCollector
	{
		internal sealed class HistogramBank
		{
			internal readonly MetricsHistogramShape Shape;
			internal readonly MetricsHistogram[] Histograms;

			internal HistogramBank(MetricsHistogramShape shape)
			{
				Shape = shape;
				Histograms = new MetricsHistogram[Latency.GetMax()];

				for (int i = 0; i < Histograms.Length; i++)
				{
					Histograms[i] = new MetricsHistogram(shape);
				}
			}

			internal HistogramBank(
				MetricsHistogramShape shape,
				MetricsHistogram[] histograms)
			{
				Shape = shape;
				Histograms = histograms;
			}
		}

		/// <summary>
		/// Replacement bank allocated before a policy cutover. Commit publishes it;
		/// the bank it replaced is retained so a rollback can re-adopt its buckets.
		/// </summary>
		internal sealed class ShapeUpdate
		{
			internal readonly MetricsNamespaceCollector Collector;
			internal readonly HistogramBank Replacement;
			internal HistogramBank Previous;

			internal ShapeUpdate(
				MetricsNamespaceCollector collector,
				MetricsHistogramShape shape)
			{
				Collector = collector;
				Replacement = new HistogramBank(shape);
			}
		}

		private long errors;
		private long timeouts;
		private long clientTimeouts;
		private long serverTimeouts;
		private long keyBusy;
		private long recordTooBig;
		private long deviceOverload;
		private long bytesIn;
		private long bytesOut;
		private volatile HistogramBank histogramBank;
		private readonly object configLock = new();
		private Action changed;

		internal MetricsNamespaceCollector(
			MetricsHistogramShape shape,
			Action changed = null)
		{
			histogramBank = new HistogramBank(shape);
			this.changed = changed;
		}

		internal void SetChangedObserver(Action observer)
		{
			Volatile.Write(ref changed, observer);
		}

		internal void AddBytes(long inCount, long outCount)
		{
			Interlocked.Add(ref bytesIn, inCount);
			Interlocked.Add(ref bytesOut, outCount);
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddLatency(
			LatencyType type,
			long elapsedNanoseconds,
			MetricsHistogramShape expected)
		{
			if (type != LatencyType.NONE)
			{
				HistogramBank current = EnsureShapeBank(expected);
				current.Histograms[(int)type].AddNanoseconds(elapsedNanoseconds);
				Volatile.Read(ref changed)?.Invoke();
			}
		}

		internal void EnsureShape(MetricsHistogramShape shape)
		{
			EnsureShapeBank(shape);
		}

		private HistogramBank EnsureShapeBank(MetricsHistogramShape shape)
		{
			HistogramBank current = histogramBank;

			if (!current.Shape.ShouldReplaceWith(shape))
			{
				return current;
			}

			lock (configLock)
			{
				current = histogramBank;

				if (current.Shape.ShouldReplaceWith(shape))
				{
					current = new HistogramBank(shape);
					histogramBank = current;
				}
				return current;
			}
		}

		internal ShapeUpdate PrepareShape(MetricsHistogramShape shape)
		{
			return new ShapeUpdate(this, shape);
		}

		internal void CommitShape(ShapeUpdate update)
		{
			lock (configLock)
			{
				HistogramBank current = histogramBank;

				if (current.Shape.ShouldReplaceWith(update.Replacement.Shape))
				{
					update.Previous = current;
					histogramBank = update.Replacement;
				}
			}
		}

		/// <summary>
		/// Move forward to <paramref name="restored"/>, re-adopting the buckets the
		/// committed update replaced when they were valid for
		/// <paramref name="previous"/>. Otherwise start an empty bank.
		/// </summary>
		internal void RollbackShape(
			ShapeUpdate update,
			MetricsHistogramShape previous,
			MetricsHistogramShape restored)
		{
			lock (configLock)
			{
				HistogramBank current = histogramBank;

				if (!current.Shape.ShouldReplaceWith(restored))
				{
					return;
				}

				bool reuse = update != null &&
					ReferenceEquals(update.Collector, this) &&
					ReferenceEquals(current, update.Replacement) &&
					update.Previous != null &&
					update.Previous.Shape.CanBeReadAs(previous) &&
					update.Previous.Shape.Matches(restored);

				histogramBank = reuse
					? new HistogramBank(restored, update.Previous.Histograms)
					: new HistogramBank(restored);
			}
		}

		internal void AddFailure(MetricsFailure failure)
		{
			if (failure == MetricsFailure.None)
			{
				return;
			}

			switch (failure)
			{
				case MetricsFailure.Timeout:
					Interlocked.Increment(ref timeouts);
					break;

				case MetricsFailure.KeyBusy:
					Interlocked.Increment(ref keyBusy);
					break;

				case MetricsFailure.RecordTooBig:
					Interlocked.Increment(ref recordTooBig);
					break;

				case MetricsFailure.DeviceOverload:
					Interlocked.Increment(ref deviceOverload);
					break;

				case MetricsFailure.Error:
					Interlocked.Increment(ref errors);
					break;
			}
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddTimeoutSplit(bool client)
		{
			if (client)
			{
				Interlocked.Increment(ref clientTimeouts);
			}
			else
			{
				Interlocked.Increment(ref serverTimeouts);
			}
			Volatile.Read(ref changed)?.Invoke();
		}

		internal MetricsNamespaceView Read(string ns, MetricsRuntimeConfig config)
		{
			return Read(ns, config.Shape);
		}

		/// <summary>
		/// Copy the published bank without reshaping it. A bank that is not valid
		/// for <paramref name="expected"/> is reported as empty buckets of the
		/// expected shape, which is what the next write-time cutover would leave.
		/// </summary>
		internal MetricsNamespaceView Read(
			string ns,
			MetricsHistogramShape expected)
		{
			Dictionary<LatencyType, long[]> latency = new();
			HistogramBank bank = histogramBank;
			MetricsHistogram[] current = bank.Histograms;
			bool valid = bank.Shape.CanBeReadAs(expected);

			for (int i = 0; i < current.Length; i++)
			{
				latency[(LatencyType)i] = valid
					? current[i].CopyBuckets()
					: new long[expected.Columns];
			}

			Dictionary<LatencyType, IReadOnlyList<long>> immutableLatency = new(latency.Count);

			foreach (KeyValuePair<LatencyType, long[]> entry in latency)
			{
				immutableLatency[entry.Key] = Array.AsReadOnly(entry.Value);
			}

			return new MetricsNamespaceView(
				ns,
				Volatile.Read(ref errors),
				Volatile.Read(ref timeouts),
				Volatile.Read(ref clientTimeouts),
				Volatile.Read(ref serverTimeouts),
				Volatile.Read(ref keyBusy),
				Volatile.Read(ref recordTooBig),
				Volatile.Read(ref deviceOverload),
				Volatile.Read(ref bytesIn),
				Volatile.Read(ref bytesOut),
				new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(immutableLatency));
		}

		internal void Merge(MetricsNamespaceView source, MetricsHistogramShape expected)
		{
			Interlocked.Add(ref errors, source.Errors);
			Interlocked.Add(ref timeouts, source.Timeouts);
			Interlocked.Add(ref clientTimeouts, source.ClientTimeouts);
			Interlocked.Add(ref serverTimeouts, source.ServerTimeouts);
			Interlocked.Add(ref keyBusy, source.KeyBusy);
			Interlocked.Add(ref recordTooBig, source.RecordTooBig);
			Interlocked.Add(ref deviceOverload, source.DeviceOverload);
			Interlocked.Add(ref bytesIn, source.BytesIn);
			Interlocked.Add(ref bytesOut, source.BytesOut);

			HistogramBank bank = EnsureShapeBank(expected);

			foreach (KeyValuePair<LatencyType, IReadOnlyList<long>> entry in source.Latency)
			{
				bank.Histograms[(int)entry.Key].AddBuckets(entry.Value);
			}
		}
	}

	internal sealed class MetricsNodeCollector
	{
		/// <summary>
		/// Shape and connection histogram published together so a reader never
		/// pairs one shape with another shape's buckets.
		/// </summary>
		internal sealed class ConnectionHistogram
		{
			internal readonly MetricsHistogramShape Shape;
			internal readonly MetricsHistogram Histogram;

			internal ConnectionHistogram(MetricsHistogramShape shape)
			{
				Shape = shape;
				Histogram = new MetricsHistogram(shape);
			}

			internal ConnectionHistogram(
				MetricsHistogramShape shape,
				MetricsHistogram histogram)
			{
				Shape = shape;
				Histogram = histogram;
			}
		}

		/// <summary>
		/// Replacement connection histogram and namespace banks allocated before a
		/// policy cutover. Namespaces created after preparation are reshaped by
		/// the write-time guard during commit.
		/// </summary>
		internal sealed class ShapeUpdate
		{
			internal readonly MetricsNodeCollector Collector;
			internal readonly ConnectionHistogram Replacement;
			internal readonly Dictionary<
				MetricsNamespaceCollector,
				MetricsNamespaceCollector.ShapeUpdate> Namespaces;
			internal ConnectionHistogram Previous;

			internal ShapeUpdate(
				MetricsNodeCollector collector,
				MetricsHistogramShape shape)
			{
				Collector = collector;
				Replacement = new ConnectionHistogram(shape);
				Namespaces = new(ReferenceEqualityComparer.Instance);

				foreach (MetricsNamespaceCollector ns in collector.namespaces.Values)
				{
					Namespaces[ns] = ns.PrepareShape(shape);
				}
			}

			internal MetricsHistogramShape Shape => Replacement.Shape;
		}

		private readonly ConcurrentDictionary<string, MetricsNamespaceCollector> namespaces = new();
		private long opened;
		private long closed;
		private long poolExhausted;
		private long openFailures;
		private long tlsHandshakeFailures;
		private long authFailures;
		private volatile ConnectionHistogram connection;
		private readonly object configLock = new();
		private Action changed;

		internal MetricsNodeCollector(MetricsRuntimeConfig config)
		{
			connection = new ConnectionHistogram(config.Shape);
		}

		internal MetricsNodeCollector(MetricsHistogramShape shape)
		{
			connection = new ConnectionHistogram(shape);
		}

		internal MetricsHistogramShape CurrentShape => connection.Shape;

		internal MetricsNamespaceCollector GetNamespace(
			string ns,
			MetricsHistogramShape expected)
		{
			MetricsHistogramShape current = connection.Shape;
			MetricsHistogramShape target =
				current.ShouldReplaceWith(expected)
					? expected
					: current;
			MetricsNamespaceCollector collector = namespaces.GetOrAdd(
				ns ?? string.Empty,
				_ => new MetricsNamespaceCollector(
					target,
					Volatile.Read(ref changed)));
			collector.SetChangedObserver(Volatile.Read(ref changed));
			collector.EnsureShape(target);
			return collector;
		}

		internal void SetChangedObserver(Action observer)
		{
			Volatile.Write(ref changed, observer);

			foreach (MetricsNamespaceCollector collector in namespaces.Values)
			{
				collector.SetChangedObserver(observer);
			}
		}

		internal void EnsureShape(MetricsHistogramShape replacement)
		{
			if (!connection.Shape.ShouldReplaceWith(replacement))
			{
				return;
			}

			lock (configLock)
			{
				if (!connection.Shape.ShouldReplaceWith(replacement))
				{
					return;
				}

				// Publish the cutover before enumeration. A namespace inserted before
				// this store is included below; one inserted afterwards observes the
				// replacement shape in GetNamespace().
				connection = new ConnectionHistogram(replacement);
				foreach (MetricsNamespaceCollector collector in namespaces.Values)
				{
					collector.EnsureShape(replacement);
				}
			}

		}

		internal ShapeUpdate PrepareShape(MetricsHistogramShape shape)
		{
			return new ShapeUpdate(this, shape);
		}

		internal void CommitShape(ShapeUpdate update)
		{
			lock (configLock)
			{
				ConnectionHistogram current = connection;

				if (!current.Shape.ShouldReplaceWith(update.Shape))
				{
					return;
				}

				// Same publication order as EnsureShape().
				update.Previous = current;
				connection = update.Replacement;
				foreach (MetricsNamespaceCollector collector in namespaces.Values)
				{
					if (update.Namespaces.TryGetValue(
						collector,
						out MetricsNamespaceCollector.ShapeUpdate prepared))
					{
						collector.CommitShape(prepared);
					}
					else
					{
						collector.EnsureShape(update.Shape);
					}
				}
			}
		}

		internal void RollbackShape(
			ShapeUpdate update,
			MetricsHistogramShape previous,
			MetricsHistogramShape restored)
		{
			lock (configLock)
			{
				ConnectionHistogram current = connection;

				if (!current.Shape.ShouldReplaceWith(restored))
				{
					return;
				}

				bool reuse = update != null &&
					ReferenceEquals(update.Collector, this) &&
					ReferenceEquals(current, update.Replacement) &&
					update.Previous != null &&
					update.Previous.Shape.CanBeReadAs(previous) &&
					update.Previous.Shape.Matches(restored);

				connection = reuse
					? new ConnectionHistogram(restored, update.Previous.Histogram)
					: new ConnectionHistogram(restored);
				foreach (MetricsNamespaceCollector collector in namespaces.Values)
				{
					MetricsNamespaceCollector.ShapeUpdate prepared = null;
					if (update != null && ReferenceEquals(update.Collector, this))
					{
						update.Namespaces.TryGetValue(collector, out prepared);
					}
					collector.RollbackShape(prepared, previous, restored);
				}
			}
		}

		internal void AddOpened()
		{
			Interlocked.Increment(ref opened);
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddClosed()
		{
			Interlocked.Increment(ref closed);
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddPoolExhausted()
		{
			Interlocked.Increment(ref poolExhausted);
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddConnectionFailure(
			ConnectionFailurePhase phase,
			bool opening)
		{
			if (opening)
			{
				Interlocked.Increment(ref openFailures);
			}

			if (phase == ConnectionFailurePhase.Tls)
			{
				Interlocked.Increment(ref tlsHandshakeFailures);
			}
			else if (phase == ConnectionFailurePhase.Auth)
			{
				Interlocked.Increment(ref authFailures);
			}
			Volatile.Read(ref changed)?.Invoke();
		}

		internal void AddConnectionLatency(
			long elapsedNanoseconds,
			MetricsHistogramShape expected)
		{
			EnsureShape(expected);
			connection.Histogram.AddNanoseconds(elapsedNanoseconds);
			Volatile.Read(ref changed)?.Invoke();
		}

		internal MetricsNodeView Read(string name, MetricsRuntimeConfig expected)
		{
			return Read(name, expected.Shape);
		}

		/// <summary>
		/// Copy the published histograms without reshaping them. Histograms not
		/// valid for <paramref name="expected"/> are reported as empty buckets of
		/// the expected shape.
		/// </summary>
		internal MetricsNodeView Read(
			string name,
			MetricsHistogramShape expected)
		{
			ConnectionHistogram current = connection;
			Dictionary<string, MetricsNamespaceView> namespaceViews = new();

			foreach (KeyValuePair<string, MetricsNamespaceCollector> entry in namespaces)
			{
				namespaceViews[entry.Key] = entry.Value.Read(entry.Key, expected);
			}

			return new MetricsNodeView(
				name,
				Volatile.Read(ref opened),
				Volatile.Read(ref closed),
				Volatile.Read(ref poolExhausted),
				Volatile.Read(ref openFailures),
				Volatile.Read(ref tlsHandshakeFailures),
				Volatile.Read(ref authFailures),
				Array.AsReadOnly(current.Shape.CanBeReadAs(expected)
					? current.Histogram.CopyBuckets()
					: new long[expected.Columns]),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(namespaceViews));
		}

		internal void Merge(MetricsNodeView source, MetricsRuntimeConfig expected)
		{
			Merge(source, expected.Shape);
		}

		internal void Merge(
			MetricsNodeView source,
			MetricsHistogramShape expected)
		{
			Interlocked.Add(ref opened, source.Opened);
			Interlocked.Add(ref closed, source.Closed);
			Interlocked.Add(ref poolExhausted, source.PoolExhausted);
			Interlocked.Add(ref openFailures, source.OpenFailures);
			Interlocked.Add(ref tlsHandshakeFailures, source.TlsHandshakeFailures);
			Interlocked.Add(ref authFailures, source.AuthFailures);
			EnsureShape(expected);
			connection.Histogram.AddBuckets(source.ConnectionLatency);

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in source.Namespaces)
			{
				GetNamespace(entry.Key, expected).Merge(entry.Value, expected);
			}
			Volatile.Read(ref changed)?.Invoke();
		}
	}
}
