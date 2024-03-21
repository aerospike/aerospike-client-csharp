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
	/// Client periodic metrics configuration.
	/// </summary>
	public sealed class MetricsPolicy
	{
		/// <summary>
		/// Listener that handles metrics notification events. The default listener implementation
		/// writes the metrics snapshot to a file which will later be read and forwarded to
		/// OpenTelemetry by a separate offline application.
		/// <para>
		/// The listener could be overridden to send the metrics snapshot directly to OpenTelemetry.
		/// </para>
		/// </summary>
		public IMetricsListener Listener;

		/// <summary>
		/// Directory path to write metrics log files for listeners that write logs.
		/// Default: current directory
		/// </summary>
		public string ReportDir = ".";

		/// <summary>
		/// Metrics file size soft limit in bytes for listeners that write logs.
		/// <para>
		/// When reportSizeLimit is reached or exceeded, the current metrics file is closed and a new
		/// metrics file is created with a new timestamp.If reportSizeLimit is zero, the metrics file
		/// size is unbounded and the file will only be closed when
		/// {@link com.aerospike.client.AerospikeClient#disableMetrics()} or TODO
		/// {
		/// link com.aerospike.client.AerospikeClient#close()} is called.
		/// </para>
		/// Default: 0
		/// </summary>
		public long ReportSizeLimit = 0;

		/// <summary>
		/// Number of cluster tend iterations between metrics notification events. One tend iteration
		/// is defined as {@link ClientPolicy#tendInterval} (default 1 second) plus the time to tend all
		/// nodes.
		/// Default: 30
		/// </summary>
		public int Interval = 30;

		/// <summary>
		/// Number of elapsed time range buckets in latency histograms.
		/// Default: 7
		/// </summary>
		public int LatencyColumns = 7;

		/// <summary>
		/// Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
		/// are in milliseconds.The first 2 buckets are &lt;=1ms and &gt;1ms. Examples:
		/// <pre>{@code
		/// // latencyColumns=7 latencyShift=1
		/// &lt;=1ms &gt;1ms &gt;2ms &gt;4ms &gt;8ms &gt;16ms &gt;32ms
		///
		/// // latencyColumns=5 latencyShift=3
		/// &lt;=1ms &gt;1ms &gt;8ms &gt;64ms &gt;512ms
		///
		/// }</pre>
		/// Default: 1
		/// </summary>
		public int LatencyShift = 1;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public MetricsPolicy(MetricsPolicy other)
		{
			this.Listener = other.Listener;
			this.ReportDir = other.ReportDir;
			this.ReportSizeLimit = other.ReportSizeLimit;
			this.Interval = other.Interval;
			this.LatencyColumns = other.LatencyColumns;
			this.LatencyShift = other.LatencyShift;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public MetricsPolicy()
		{
		}

		public void SetListener(IMetricsListener listener)
		{
			this.Listener = listener;
		}

		public void SetReportDir(String reportDir)
		{
			this.ReportDir = reportDir;
		}

		public void SetReportSizeLimit(long reportSizeLimit)
		{
			this.ReportSizeLimit = reportSizeLimit;
		}

		public void SetInterval(int interval)
		{
			this.Interval = interval;
		}

		public void SetLatencyColumns(int latencyColumns)
		{
			this.LatencyColumns = latencyColumns;
		}

		public void SetLatencyShift(int latencyShift)
		{
			this.LatencyShift = latencyShift;
		}
	}
}
