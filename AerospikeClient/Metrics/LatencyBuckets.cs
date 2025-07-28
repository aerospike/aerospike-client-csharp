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

namespace Aerospike.Client
{
	/// <summary>
	/// Latency buckets for a command group (See <see cref="Latency.LatencyType"/>).
	/// Latency bucket counts are cumulative and not reset on each metrics snapshot interval.
	/// </summary>
	public sealed class LatencyBuckets
	{
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
		/// Increment count of bucket corresponding to the elapsed time in milliseconds.
		/// </summary>
		public void Add(double elapsed)
		{
			int index = GetIndex(elapsed);
			Interlocked.Increment(ref buckets[index]);
		}

		private int GetIndex(double elapsed)
		{
			// Round up elapsed to nearest millisecond.
			long elapsedRounded = (long)Math.Ceiling(elapsed);

			int lastBucket = buckets.Length - 1;
			long limit = 1;

			for (int i = 0; i < lastBucket; i++)
			{
				if (elapsedRounded <= limit)
				{
					return i;
				}
				limit <<= latencyShift;
			}
			return lastBucket;
		}
	}
}
