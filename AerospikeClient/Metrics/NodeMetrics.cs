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
	public sealed class NodeMetrics
	{
		public Histograms Histograms { get; private set; }
		public readonly Counter BytesInCounter;
		public readonly Counter BytesOutCounter;

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
		/// Add elapsed time in nanoseconds to latency buckets corresponding to latency type.
		/// This is where the conversion to nanoseconds occurs.
		/// </summary>
		/// <param name="ns">namespace</param>
		/// <param name="type"></param>
		/// <param name="elapsedMs">elapsed time, in milliseconds</param>
		public void AddLatency(string ns, LatencyType type, double elapsedMs) {
			Histograms.AddLatency(ns, type, (long)elapsedMs * LatencyBuckets.NS_TO_MS);
		}
	}
}
