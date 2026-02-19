/* 
 * Copyright 2012-2026 Aerospike, Inc.
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

using Aerospike.Client.Config;

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
		[Obsolete("Use Exporters instead. This property is maintained for backward compatibility.")]
		public IMetricsListener Listener;

		/// <summary>
		/// List of metrics exporters that will receive metrics data periodically.
		/// Each exporter can send metrics to different destinations (files, OpenTelemetry, Prometheus, etc.).
		/// If no exporters are configured and Listener is also null, a default MetricsWriter will be used.
		/// </summary>
		public List<IMetricsExporter> Exporters { get; set; } = new List<IMetricsExporter>();

		/// <summary>
		/// Directory path to write metrics log files for listeners that write logs.
		/// Default: current directory
		/// </summary>
		/// <remarks>
		/// When using the new IMetricsExporter API, configure this setting directly on MetricsWriter instead.
		/// This property is only used by the legacy IMetricsListener API.
		/// </remarks>
		[Obsolete("When using IMetricsExporter, configure the directory directly on MetricsWriter constructor instead.")]
		public string ReportDir = ".";

		/// <summary>
		/// Metrics file size soft limit in bytes for listeners that write logs.
		/// <para>
		/// When reportSizeLimit is reached or exceeded, the current metrics file is closed and a new
		/// metrics file is created with a new timestamp.If reportSizeLimit is zero, the metrics file
		/// size is unbounded and the file will only be closed when
		/// <see cref="AerospikeClient.DisableMetrics()"/> or
		/// <see cref="AerospikeClient.Close()"/> is called.
		/// </para>
		/// Default: 0
		/// </summary>
		/// <remarks>
		/// When using the new IMetricsExporter API, configure this setting directly on MetricsWriter instead.
		/// This property is only used by the legacy IMetricsListener API.
		/// </remarks>
		[Obsolete("When using IMetricsExporter, configure the size limit directly on MetricsWriter constructor instead.")]
		public long ReportSizeLimit = 0;

		/// <summary>
		/// Number of cluster tend iterations between metrics notification events. One tend iteration
		/// is defined as <see cref="ClientPolicy.tendInterval"/> (default 1 second) plus the time to tend all
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
		/// Labels that can be sent to the metrics output
		/// </summary>
		public Dictionary<string, string> labels;

		internal bool restartRequired = false;

		/// <summary>
		/// Copy metrics policy from another metrics policy AND override certain policy attributes if they exist in the
		/// configProvider.
		/// </summary>
		public MetricsPolicy(MetricsPolicy other, IConfigurationData config) :
			this(other)
		{
			if (config == null)
			{
				return;
			}

			var metrics = config.dynamicConfig.metrics;
			if (metrics == null)
			{
				return;
			}

			if (metrics.labels != null)
			{
				this.labels = new Dictionary<string, string>(metrics.labels);
			}
			if (metrics.latency_shift.HasValue)
			{
				if (metrics.latency_shift.Value != this.LatencyShift)
				{
					restartRequired = true;
				}
				this.LatencyShift = metrics.latency_shift.Value;
			}
			if (metrics.latency_columns.HasValue)
			{
				if (metrics.latency_columns.Value != this.LatencyColumns)
				{
					restartRequired = true;
				}
				this.LatencyColumns = metrics.latency_columns.Value;
			}
			if (LatencyColumns < 1)
			{
				Log.Error("An invalid # of latency columns was provided. Setting latency columns to default (7).");
				LatencyColumns = 7;
			}
		}

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public MetricsPolicy(MetricsPolicy other)
		{
			#pragma warning disable CS0618 // Type or member is obsolete
			this.Listener = other.Listener;
			this.ReportDir = other.ReportDir;
			this.ReportSizeLimit = other.ReportSizeLimit;
			#pragma warning restore CS0618
			this.Exporters = new List<IMetricsExporter>(other.Exporters);
			this.Interval = other.Interval;
			this.LatencyColumns = other.LatencyColumns;
			this.LatencyShift = other.LatencyShift;
			this.labels = other.labels;
			this.restartRequired = other.restartRequired;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public MetricsPolicy()
		{
		}

		/// <summary>
		/// Set the legacy listener. Deprecated - use AddExporter instead.
		/// </summary>
		[Obsolete("Use AddExporter instead")]
		public void SetListener(IMetricsListener listener)
		{
			#pragma warning disable CS0618 // Type or member is obsolete
			this.Listener = listener;
			#pragma warning restore CS0618
		}

		/// <summary>
		/// Add a metrics exporter to the list of exporters.
		/// </summary>
		/// <param name="exporter">The exporter to add</param>
		public void AddExporter(IMetricsExporter exporter)
		{
			Exporters.Add(exporter);
		}

		/// <summary>
		/// Remove a metrics exporter from the list of exporters.
		/// </summary>
		/// <param name="exporter">The exporter to remove</param>
		/// <returns>True if the exporter was found and removed</returns>
		public bool RemoveExporter(IMetricsExporter exporter)
		{
			return Exporters.Remove(exporter);
		}

		/// <summary>
		/// Clear all exporters.
		/// </summary>
		public void ClearExporters()
		{
			Exporters.Clear();
		}

		/// <summary>
		/// Set the directory path for metrics log files.
		/// </summary>
		[Obsolete("When using IMetricsExporter, configure the directory directly on MetricsWriter constructor instead.")]
		public void SetReportDir(String reportDir)
		{
			#pragma warning disable CS0618 // Type or member is obsolete
			this.ReportDir = reportDir;
			#pragma warning restore CS0618
		}

		/// <summary>
		/// Set the metrics file size limit.
		/// </summary>
		[Obsolete("When using IMetricsExporter, configure the size limit directly on MetricsWriter constructor instead.")]
		public void SetReportSizeLimit(long reportSizeLimit)
		{
			#pragma warning disable CS0618 // Type or member is obsolete
			this.ReportSizeLimit = reportSizeLimit;
			#pragma warning restore CS0618
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
