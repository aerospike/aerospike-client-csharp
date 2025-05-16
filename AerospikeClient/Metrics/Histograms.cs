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
	public class Histograms
	{
		private readonly ConcurrentHashMap<string, LatencyBuckets[]> histoMap = new();
		private readonly int histoShift;
		private readonly int columnCount;
		private readonly static string noNsLabel = "";
		private readonly int max;

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

		public void AddLatency(string ns, Latency.LatencyType type, long elapsed)
		{
			if (ns == null)
			{
				ns = noNsLabel;
			}

			LatencyBuckets[] buckets = histoMap[ns];
			if (buckets == null)
			{
				buckets = CreateBuckets();
				LatencyBuckets[] finalBuckets = buckets;
				histoMap.SetValueIfNotNull(ns, finalBuckets);
			}

			buckets[(int)type].Add(elapsed);
		}

		public LatencyBuckets[] GetBuckets(string ns)
		{
			return histoMap[ns];
		}
	}	
}
