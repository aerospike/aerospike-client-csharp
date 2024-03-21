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

using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	/// <summary>
	/// Client metrics listener.
	/// </summary>
	public sealed class NodeMetrics
	{
		private readonly LatencyBuckets[] Latency;

		/// <summary>
		/// Initialize extended node metrics.
		/// </summary>
		public NodeMetrics(MetricsPolicy policy)
		{
			int latencyColumns = policy.LatencyColumns;
			int latencyShift = policy.LatencyShift;
			int max = Client.Latency.GetMax();

			Latency = new LatencyBuckets[max];

			for (int i = 0; i < max; i++)
			{
				Latency[i] = new LatencyBuckets(latencyColumns, latencyShift);
			}
		}

		/// <summary>
		/// Add elapsed time in nanoseconds to latency buckets corresponding to latency type.
		/// </summary>
		public void AddLatency(LatencyType type, long elapsed)
		{
			Latency[(int)type].Add(elapsed);
		}

		/// <summary>
		/// Return latency buckets given type.
		/// </summary>
		public LatencyBuckets GetLatencyBuckets(int type)
		{
			return Latency[type];
		}
	}
}
