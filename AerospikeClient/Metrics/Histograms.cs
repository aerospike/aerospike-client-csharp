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
	/// A Histograms object is a container for a map of namespaces to histograms (as defined by their associated
	/// LatencyBuckets) and their histogram properties
	/// </summary>
	public class Histograms
	{
		internal readonly ConcurrentHashMap<string, LatencyBuckets[]> histoMap = new();
		private readonly int histoShift;
		private readonly int columnCount;
		private readonly static string noNsLabel = "";
		private readonly int max;

		/// <summary>
		/// Create Histograms object
		/// </summary>
		/// <param name="columnCount">number of histogram columns or "buckets"</param>
		/// <param name="shift"> power of 2 multiple between each range bucket in histogram starting at bucket 3.
		/// The first 2 buckets are "&lt;=1ms" and "&gt;1ms".</param>
		public Histograms(int columnCount, int shift)
		{
			this.columnCount = columnCount;
			this.histoShift = shift;
			max = Latency.GetMax();
		}

		private LatencyBuckets[] CreateBuckets()
		{
			LatencyBuckets[] buckets = new LatencyBuckets[max];

			for (int i = 0; i < max; i++)
			{
				buckets[i] = new LatencyBuckets(columnCount, histoShift);
			}

			return buckets;
		}

		/// <summary>
		/// Increment count of bucket corresponding to the namespace elapsed time in nanoseconds.
		/// </summary>
		/// <param name="ns"></param>
		/// <param name="type"></param>
		/// <param name="elapsed"></param>
		public void AddLatency(string ns, Latency.LatencyType type, long elapsed)
		{
			ns ??= noNsLabel;

			LatencyBuckets[] buckets;
			if (!histoMap.ContainsKey(ns))
			{
				buckets = CreateBuckets();
				LatencyBuckets[] finalBuckets = buckets;
				histoMap.SetValueIfNotNull(ns, finalBuckets);
			}
			else
			{
				buckets = histoMap[ns];
			}

			buckets[(int)type].Add(elapsed);
		}

		public LatencyBuckets[] GetBuckets(string ns)
		{
			return histoMap[ns];
		}
	}	
}
