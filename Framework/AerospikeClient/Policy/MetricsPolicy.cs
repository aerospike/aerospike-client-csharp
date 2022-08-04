/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	/// Client metrics configuration.
	/// </summary>
	public class MetricsPolicy
	{
		/// <summary>
		/// File path to append cluster statistics and latency histograms.
		/// </summary>
		public string reportPath;

		/// <summary>
		/// Number of cluster tend iterations between statistics log messages. One tend iteration is defined as
		/// <see cref="Aerospike.Client.ClientPolicy.tendInterval"/> (default 1 second) plus the time to tend all nodes.
		/// <para>Default: 30</para>
		/// </summary>
		public uint reportInterval = 30;

		/// <summary>
		/// Number of elapsed time range buckets in latency histograms.
		/// <para>Default: 5</para>
		/// </summary>
		public int latencyColumns = 5;

		/// <summary>
		/// Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
		/// are in milliseconds. The first 2 buckets are "&lt;=1ms" and "&gt;1ms".
		/// <para>
		/// For example, latencyColumns=5 and latencyShift=3 produces the following histogram buckets:
		/// </para>
		/// <para>
		/// <code>
		/// &lt;=1ms &gt;1ms &gt;8ms &gt;64ms &gt;512ms
		/// </code>
		/// </para>
		/// <para>Default: 3</para>
		/// </summary>
		public int latencyShift = 3;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public MetricsPolicy(MetricsPolicy other)
		{
			this.reportPath = other.reportPath;
			this.reportInterval = other.reportInterval;
			this.latencyColumns = other.latencyColumns;
			this.latencyShift = other.latencyShift;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public MetricsPolicy()
		{
		}
	}
}
