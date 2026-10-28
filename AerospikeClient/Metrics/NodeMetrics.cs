/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using System.Collections.ObjectModel;

namespace Aerospike.Client
{
	/// <summary>
	/// Client metrics listener.
	/// </summary>
	public sealed class NodeMetrics : IDisposable
	{
		public Histograms Histograms { get; private set; }
		public Counter BytesInCounter;
		public Counter BytesOutCounter;
		private readonly object operationalLock = new();
		private int disposed;

		/// <summary>
		/// Initialize extended node metrics.
		/// </summary>
		public NodeMetrics(MetricsPolicy policy)
		{
			int latencyColumns = policy.LatencyColumns;
			int latencyShift = policy.LatencyShift;
			this.BytesInCounter = new Counter();
			this.BytesOutCounter = new Counter();

			Histograms = new Histograms(
				latencyColumns,
				latencyShift,
				policy.LatencyUnit);
		}

		/// <summary>
		/// Add elapsed time in milliseconds to latency buckets corresponding to latency type.
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="type"></param>
		/// <param name="elapsedMs">elapsed time, in milliseconds</param>
		public void AddLatency(string ns, LatencyType type, double elapsedMs)
		{
			lock (operationalLock)
			{
				Histograms?.AddLatency(ns, type, elapsedMs);
			}
		}

		internal void Reshape(MetricsPolicy policy)
		{
			if (Histograms?.Matches(policy) == false)
			{
				ReplaceHistograms(CreateHistograms(policy));
			}
		}

		internal static Histograms CreateHistograms(
			MetricsPolicy policy)
		{
			return new Histograms(
				policy.LatencyColumns,
				policy.LatencyShift,
				policy.LatencyUnit);
		}

		internal Histograms ReplaceHistograms(
			Histograms replacement)
		{
			lock (operationalLock)
			{
				// Do not dispose the previous map here. A command that captured it before
				// the swap may still be recording latency. It becomes collectible when
				// those in-flight readers release it.
				Histograms previous = Histograms;
				Histograms = replacement;
				return previous;
			}
		}

		internal void RecordSuccess(
			IReadOnlyList<string> namespaces,
			string bytesNamespace,
			LatencyType type,
			long elapsedNanoseconds,
			long bytesIn,
			long bytesOut)
		{
			lock (operationalLock)
			{
				if (bytesNamespace != null)
				{
					BytesInCounter?.Increment(bytesNamespace, bytesIn);
					BytesOutCounter?.Increment(bytesNamespace, bytesOut);
				}

				if (elapsedNanoseconds > 0)
				{
					foreach (string ns in namespaces)
					{
						Histograms?.AddLatencyNanoseconds(
							ns,
							type,
							elapsedNanoseconds);
					}
				}
			}
		}

		internal MetricsNodeView Read(string name)
		{
			lock (operationalLock)
			{
				return ReadUnlocked(name);
			}
		}

		private MetricsNodeView ReadUnlocked(string name)
		{
			HashSet<string> namespaceNames = new(StringComparer.Ordinal);
			Counter bytesIn = BytesInCounter;
			Counter bytesOut = BytesOutCounter;
			Histograms histograms = Histograms;
			HashSet<string> bytesInNames = bytesIn == null
				? new HashSet<string>(StringComparer.Ordinal)
				: new HashSet<string>(
					bytesIn.GetNamespaces(),
					StringComparer.Ordinal);
			HashSet<string> bytesOutNames = bytesOut == null
				? new HashSet<string>(StringComparer.Ordinal)
				: new HashSet<string>(
					bytesOut.GetNamespaces(),
					StringComparer.Ordinal);

			namespaceNames.UnionWith(bytesInNames);
			namespaceNames.UnionWith(bytesOutNames);
			if (histograms != null)
			{
				namespaceNames.UnionWith(histograms.GetNamespaces());
			}

			Dictionary<string, MetricsNamespaceView> namespaces =
				new(StringComparer.Ordinal);
			int columns = histograms?.GetShape(0).Columns ?? 0;
			long[] connectionLatency = new long[columns];

			foreach (string ns in namespaceNames)
			{
				Dictionary<LatencyType, IReadOnlyList<long>> latency = new();
				LatencyBuckets[] buckets =
					histograms?.histoMap.ContainsKey(ns) == true
						? histograms.GetBuckets(ns)
						: null;

				bool hasNamespaceLatency = false;

				for (int i = 0; i < Latency.GetMax(); i++)
				{
					long[] copy = buckets == null
						? new long[columns]
						: Enumerable.Range(0, buckets[i].GetMax())
							.Select(index => buckets[i].GetBucket(index))
							.ToArray();

					if ((LatencyType)i == LatencyType.CONN)
					{
						for (int bucket = 0;
							bucket < Math.Min(connectionLatency.Length, copy.Length);
							bucket++)
						{
							connectionLatency[bucket] += copy[bucket];
						}
						latency[(LatencyType)i] =
							Array.AsReadOnly(new long[columns]);
					}
					else
					{
						hasNamespaceLatency |= copy.Any(value => value != 0);
						latency[(LatencyType)i] = Array.AsReadOnly(copy);
					}
				}

				if (bytesInNames.Contains(ns) ||
					bytesOutNames.Contains(ns) ||
					hasNamespaceLatency)
				{
					namespaces[ns] = new MetricsNamespaceView(
						ns,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						bytesIn?.GetCountByNS(ns) ?? 0,
						bytesOut?.GetCountByNS(ns) ?? 0,
						new ReadOnlyDictionary<
							LatencyType,
							IReadOnlyList<long>>(latency));
				}
			}

			return new MetricsNodeView(
				name,
				0,
				0,
				0,
				0,
				0,
				0,
				Array.AsReadOnly(connectionLatency),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(
					namespaces));
		}

		internal MetricsNodeView Read(
			string name,
			MetricsHistogramShape expected)
		{
			lock (operationalLock)
			{
				MetricsNodeView view = ReadUnlocked(name);
				return Histograms?.Matches(expected) != false
					? view
					: MetricsNodeNameState.NormalizeShape(
						view,
						Histograms.GetShape(expected.Generation),
						expected);
			}
		}

		private void Dispose(bool disposing)
		{
			if (Interlocked.Exchange(ref disposed, 1) == 0)
			{
				if (disposing)
				{
					BytesInCounter?.Dispose();
					BytesInCounter = null;
					BytesOutCounter?.Dispose();
					BytesOutCounter = null;
					Histograms?.Dispose();
					Histograms = null;
				}

			}
		}

		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}
	}

	internal sealed class NodeMetricsSlot
	{
		private int readers = 1;
		private int retired;
		internal readonly NodeMetrics Metrics;

		internal NodeMetricsSlot(NodeMetrics metrics)
		{
			Metrics = metrics;
		}

		internal NodeMetricsLease Acquire()
		{
			while (true)
			{
				if (Volatile.Read(ref retired) != 0)
				{
					return default;
				}

				int current = Volatile.Read(ref readers);

				if (current == 0)
				{
					return default;
				}

				if (Interlocked.CompareExchange(
					ref readers,
					current + 1,
					current) == current)
				{
					if (Volatile.Read(ref retired) == 0)
					{
						return new NodeMetricsLease(this, Metrics);
					}

					Release();
					return default;
				}
			}
		}

		internal void Retire()
		{
			if (Interlocked.Exchange(ref retired, 1) == 0)
			{
				Release();
			}
		}

		internal T RetireAndCapture<T>(Func<NodeMetrics, T> capture)
		{
			if (Interlocked.Exchange(ref retired, 1) != 0)
			{
				return default;
			}

			SpinWait spinner = new();
			while (Volatile.Read(ref readers) != 1)
			{
				spinner.SpinOnce();
			}

			try
			{
				return capture(Metrics);
			}
			finally
			{
				Release();
			}
		}

		internal void Release()
		{
			if (Interlocked.Decrement(ref readers) == 0)
			{
				Metrics.Dispose();
			}
		}
	}

	internal readonly struct NodeMetricsLease : IDisposable
	{
		private readonly NodeMetricsSlot slot;
		internal readonly NodeMetrics Metrics;

		internal NodeMetricsLease(NodeMetricsSlot slot, NodeMetrics metrics)
		{
			this.slot = slot;
			Metrics = metrics;
		}

		public void Dispose()
		{
			slot?.Release();
		}
	}
}
