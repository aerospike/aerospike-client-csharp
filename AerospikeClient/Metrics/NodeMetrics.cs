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

			Histograms = new Histograms(latencyColumns, latencyShift);
		}

		/// <summary>
		/// Add elapsed time in milliseconds to latency buckets corresponding to latency type.
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="type"></param>
		/// <param name="elapsedMs">elapsed time, in milliseconds</param>
		public void AddLatency(string ns, LatencyType type, double elapsedMs)
		{
			Histograms?.AddLatency(ns, type, elapsedMs);
		}

		internal void Reshape(MetricsPolicy policy)
		{
			Histograms replacement = new Histograms(policy.LatencyColumns, policy.LatencyShift);

			// Do not dispose the previous map here. A command that captured it before
			// the swap may still be recording latency. It becomes collectible when
			// those in-flight readers release it.
			Histograms = replacement;
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
					return new NodeMetricsLease(this, Metrics);
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
