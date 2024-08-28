/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	/// <summary>
	/// Latency buckets for a command group (See {@link com.aerospike.client.metrics.LatencyType}).
	/// Latency bucket counts are cumulative and not reset on each metrics snapshot interval.
	/// </summary>
	public sealed class LatencyBuckets
	{
		private const long NS_TO_MS = 1000000;

		private volatile int[] buckets;
		private readonly int latencyShift;

		/// <summary>
		/// Initialize latency buckets.
		/// </summary>
		/// <param name="latencyColumns">number of latency buckets</param>
		/// <param name="latencyShift">power of 2 multiple between each range bucket in latency histograms starting at bucket 3.
		/// 							The first 2 buckets are &lt;=1ms and &gt;1ms.</param>
		public LatencyBuckets(int latencyColumns, int latencyShift)
		{
			this.latencyShift = latencyShift;
			buckets = new int[latencyColumns];
		}

		/// <summary>
		/// Return number of buckets.
		/// </summary>
		public int GetMax()
		{
			return buckets.Length;
		}

		/// <summary>
		/// Return cumulative count of a bucket.
		/// </summary>
		public long GetBucket(int i)
		{
			return Volatile.Read(ref buckets[i]);
		}

		/// <summary>
		/// Increment count of bucket corresponding to the elapsed time in nanoseconds.
		/// </summary>
		public void Add(long elapsed)
		{
			int index = GetIndex(elapsed);
			Interlocked.Increment(ref buckets[index]);
		}

		private int GetIndex(long elapsedNanos)
		{
			// Convert nanoseconds to milliseconds.
			long elapsed = elapsedNanos / NS_TO_MS;

			// Round up elapsed to nearest millisecond.
			if ((elapsedNanos - (elapsed * NS_TO_MS)) > 0)
			{
				elapsed++;
			}

			int lastBucket = buckets.Length - 1;
			long limit = 1;

			for (int i = 0; i < lastBucket; i++)
			{
				if (elapsed <= limit)
				{
					return i;
				}
				limit <<= latencyShift;
			}
			return lastBucket;
		}
	}
}
