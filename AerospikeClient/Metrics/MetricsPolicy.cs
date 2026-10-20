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

using Aerospike.Client.Config;
using System.Globalization;

namespace Aerospike.Client
{
	/// <summary>
	/// Unit used to convert monotonic elapsed time before v3 latency bucketing.
	/// </summary>
	public enum MetricsLatencyUnit
	{
		/// <summary>
		/// Convert elapsed time to whole milliseconds.
		/// </summary>
		Milliseconds,

		/// <summary>
		/// Convert elapsed time to whole microseconds.
		/// </summary>
		Microseconds
	}

	/// <summary>
	/// Record-time command sampler configuration.
	/// </summary>
	public sealed class MetricsSampler
	{
		/// <summary>
		/// Sampler denominator. Must be at least one.
		/// </summary>
		public ulong Range = 1;

		/// <summary>
		/// Number of values in <see cref="Range"/> that are sampled.
		/// </summary>
		public ulong Threshold = 1;

		/// <summary>
		/// Create a sampler that records the requested probability.
		/// </summary>
		public static MetricsSampler FromProbability(double probability)
		{
			if (double.IsNaN(probability) || probability < 0 || probability > 1)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics sample probability must be between 0 and 1.");
			}

			const ulong range = 1_000_000;
			ulong threshold = (ulong)Math.Round(
				probability * range,
				MidpointRounding.AwayFromZero);

			return new MetricsSampler
			{
				Range = range,
				Threshold = threshold
			};
		}

		internal MetricsSampler Copy()
		{
			return new MetricsSampler
			{
				Range = Range,
				Threshold = Threshold
			};
		}
	}

	/// <summary>
	/// Client periodic metrics configuration.
	/// </summary>
	public sealed class MetricsPolicy
	{
		/// <summary>
		/// Enable sampled operational command, connection, recover, query and tend metrics.
		/// The legacy API default is true for backward compatibility.
		/// </summary>
		public bool OperationalEnabled = true;

		/// <summary>
		/// Enable client-wide API usage counters.
		/// </summary>
		public bool UsageEnabled;

		/// <summary>
		/// Unit used by v3 latency histograms.
		/// </summary>
		public MetricsLatencyUnit LatencyUnit = MetricsLatencyUnit.Milliseconds;

		/// <summary>
		/// Record-time sampler applied once at the command sampling boundary.
		/// </summary>
		public MetricsSampler Sampler = new();

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
		/// <see cref="AerospikeClient.DisableMetrics()"/> or
		/// <see cref="AerospikeClient.Close()"/> is called.
		/// </para>
		/// Default: 0
		/// </summary>
		public long ReportSizeLimit = 0;

		/// <summary>
		/// Number of cluster tend iterations between metrics notification events. One tend iteration
		/// is defined as <see cref="ClientPolicy.tendInterval"/> (default 1 second) plus the time to tend all
		/// nodes.
		/// Default: 30
		/// </summary>
		public int Interval = 30;

		/// <summary>
		/// Duration between periodic exporter callbacks. This duration is independent of
		/// <see cref="Interval"/>, which remains the legacy listener tend-count cadence.
		/// Default: 30 seconds.
		/// </summary>
		public TimeSpan ExportInterval = TimeSpan.FromSeconds(30);

		/// <summary>
		/// Optional exclusive exporter mode. A null value preserves source-aware compatibility:
		/// an explicit legacy listener selects <see cref="MetricsExporterMode.None"/>, registered
		/// exporters select <see cref="MetricsExporterMode.Custom"/>, and the legacy programmatic
		/// default selects <see cref="MetricsExporterMode.File"/>.
		/// </summary>
		public MetricsExporterMode? Exporter;

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
		private readonly List<IMetricsExporter> exporters = new();

		/// <summary>
		/// Application-owned custom exporters in registration order.
		/// </summary>
		public IReadOnlyList<IMetricsExporter> Exporters => exporters.AsReadOnly();

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

			if (metrics.enable.HasValue && metrics.enabled.HasValue)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics configuration can not contain both 'enable' and 'enabled'.");
			}

			MetricsOperationalConfig operational = metrics.extended?.operational;
			MetricsUsageConfig usage = metrics.extended?.usage;

			if (metrics.enabled.HasValue)
			{
				// New master switch enables Tier 0 only unless children are explicit.
				this.OperationalEnabled = metrics.enabled.Value &&
					(operational?.enabled ?? false);
				this.UsageEnabled = metrics.enabled.Value &&
					(usage?.enabled ?? false);
			}
			else if (metrics.enable == true)
			{
				// Legacy master switch retains full legacy operational behavior.
				this.OperationalEnabled = operational?.enabled ?? true;
				this.UsageEnabled = usage?.enabled ?? false;
			}
			else if (metrics.enable == false)
			{
				this.OperationalEnabled = false;
				this.UsageEnabled = false;
			}
			else
			{
				if (operational?.enabled.HasValue == true)
				{
					this.OperationalEnabled = operational.enabled.Value;
				}

				if (usage?.enabled.HasValue == true)
				{
					this.UsageEnabled = usage.enabled.Value;
				}
			}

			if (metrics.labels != null)
			{
				this.labels = new Dictionary<string, string>(metrics.labels);
			}
			if (metrics.export_interval != null)
			{
				this.ExportInterval = MetricsConfigurationParser.ParseDuration(
					metrics.export_interval,
					"metrics.export_interval");
			}
			if (metrics.exporter != null)
			{
				if (!Enum.TryParse(
					metrics.exporter,
					true,
					out MetricsExporterMode exporterMode) ||
					!Enum.IsDefined(exporterMode))
				{
					throw new AerospikeException(
						ResultCode.PARAMETER_ERROR,
						"Unsupported metrics exporter mode: " + metrics.exporter);
				}
				this.Exporter = exporterMode;
			}
			else if (metrics.enabled.HasValue &&
				!this.Exporter.HasValue &&
				this.Listener == null &&
				this.exporters.Count == 0)
			{
				// The modern master switch defaults to collection without delivery.
				this.Exporter = MetricsExporterMode.None;
			}
			else if (metrics.enable.HasValue &&
				!this.Exporter.HasValue &&
				this.Listener == null &&
				this.exporters.Count == 0)
			{
				// The legacy master switch preserves the built-in file convenience.
				this.Exporter = MetricsExporterMode.File;
			}
			if (metrics.report_dir != null)
			{
				this.ReportDir = metrics.report_dir;
			}
			if (metrics.report_size_limit != null)
			{
				this.ReportSizeLimit = MetricsConfigurationParser.ParseSize(
					metrics.report_size_limit,
					"metrics.report_size_limit");
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

			if (OperationalEnabled)
			{
				if (operational?.latency_shift.HasValue == true)
				{
					restartRequired |= operational.latency_shift.Value != this.LatencyShift;
					this.LatencyShift = operational.latency_shift.Value;
				}

				if (operational?.latency_columns.HasValue == true)
				{
					restartRequired |= operational.latency_columns.Value != this.LatencyColumns;
					this.LatencyColumns = operational.latency_columns.Value;
				}

				if (operational?.latency_unit != null)
				{
					if (!Enum.TryParse(
						operational.latency_unit,
						true,
						out MetricsLatencyUnit latencyUnit))
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR,
							"Unsupported metrics latency unit: " + operational.latency_unit);
					}
					this.LatencyUnit = latencyUnit;
				}

				if (operational?.sampler?.range.HasValue == true)
				{
					this.Sampler.Range = operational.sampler.range.Value;
				}

				if (operational?.sampler?.threshold.HasValue == true)
				{
					this.Sampler.Threshold = operational.sampler.threshold.Value;
				}
			}
			Validate();
		}

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public MetricsPolicy(MetricsPolicy other)
		{
			this.OperationalEnabled = other.OperationalEnabled;
			this.UsageEnabled = other.UsageEnabled;
			this.LatencyUnit = other.LatencyUnit;
			this.Sampler = other.Sampler?.Copy() ?? new MetricsSampler();
			this.Listener = other.Listener;
			this.ReportDir = other.ReportDir;
			this.ReportSizeLimit = other.ReportSizeLimit;
			this.Interval = other.Interval;
			this.ExportInterval = other.ExportInterval;
			this.Exporter = other.Exporter;
			this.LatencyColumns = other.LatencyColumns;
			this.LatencyShift = other.LatencyShift;
			this.labels = other.labels == null
				? null
				: new Dictionary<string, string>(other.labels);
			this.restartRequired = other.restartRequired;
			this.exporters.AddRange(other.exporters);
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

		/// <summary>
		/// Set the periodic exporter cadence.
		/// </summary>
		public void SetExportInterval(TimeSpan exportInterval)
		{
			this.ExportInterval = exportInterval;
		}

		/// <summary>
		/// Set the exclusive exporter mode.
		/// </summary>
		public void SetExporter(MetricsExporterMode exporter)
		{
			this.Exporter = exporter;
		}

		/// <summary>
		/// Register an application-owned custom exporter. Duplicate object references are ignored.
		/// </summary>
		public void AddExporter(IMetricsExporter exporter)
		{
			if (exporter == null)
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics exporter is required.");
			}

			foreach (IMetricsExporter current in exporters)
			{
				if (ReferenceEquals(current, exporter))
				{
					return;
				}
			}
			exporters.Add(exporter);
		}

		public void SetLatencyColumns(int latencyColumns)
		{
			this.LatencyColumns = latencyColumns;
		}

		public void SetLatencyShift(int latencyShift)
		{
			this.LatencyShift = latencyShift;
		}

		/// <summary>
		/// Set operational collection enabled state.
		/// </summary>
		public void SetOperationalEnabled(bool enabled)
		{
			this.OperationalEnabled = enabled;
		}

		/// <summary>
		/// Set usage collection enabled state.
		/// </summary>
		public void SetUsageEnabled(bool enabled)
		{
			this.UsageEnabled = enabled;
		}

		/// <summary>
		/// Set the v3 latency histogram unit.
		/// </summary>
		public void SetLatencyUnit(MetricsLatencyUnit latencyUnit)
		{
			this.LatencyUnit = latencyUnit;
		}

		/// <summary>
		/// Set the record-time sampler.
		/// </summary>
		public void SetSampler(MetricsSampler sampler)
		{
			this.Sampler = sampler ?? throw new AerospikeException(
				ResultCode.PARAMETER_ERROR,
				"Metrics sampler is required.");
		}

		internal void Validate()
		{
			if (LatencyColumns < 1)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics latency columns must be at least one.");
			}

			if (LatencyShift < 1)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics latency shift must be positive.");
			}

			if (!Enum.IsDefined(LatencyUnit))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Unsupported metrics latency unit.");
			}

			if (Sampler == null)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics sampler is required.");
			}

			if (Sampler.Range < 1)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics sampler range must be at least one.");
			}

			if (Sampler.Threshold > Sampler.Range)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Metrics sampler threshold must be between zero and range.");
			}

			if (ExportInterval <= TimeSpan.Zero)
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics export interval must be positive.");
			}

			if (Exporter.HasValue && !Enum.IsDefined(Exporter.Value))
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Unsupported metrics exporter mode.");
			}

			if (ReportSizeLimit < 0 ||
				(ReportSizeLimit != 0 && ReportSizeLimit < 1_000_000))
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics report size limit must be zero or at least 1000000 bytes.");
			}

			foreach (IMetricsExporter exporter in exporters)
			{
				if (Listener is MetricsWriter &&
					ReferenceEquals(Listener, exporter))
				{
					throw new AerospikeException(
						ResultCode.PARAMETER_ERROR,
						"The same MetricsWriter can not be both a listener and an exporter.");
				}
			}
		}

		internal MetricsExporterMode ResolveExporterMode()
		{
			if (Exporter.HasValue)
			{
				return Exporter.Value;
			}
			if (Listener != null)
			{
				return MetricsExporterMode.None;
			}
			if (exporters.Count > 0)
			{
				return MetricsExporterMode.Custom;
			}
			return MetricsExporterMode.File;
		}
	}

	internal static class MetricsConfigurationParser
	{
		internal static TimeSpan ParseDuration(string value, string field)
		{
			string text = value?.Trim();

			if (string.IsNullOrEmpty(text))
			{
				throw Invalid(field, value);
			}

			long ticksPerUnit;
			string number;

			if (text.EndsWith("ms", StringComparison.OrdinalIgnoreCase))
			{
				ticksPerUnit = TimeSpan.TicksPerMillisecond;
				number = text[..^2];
			}
			else if (text.EndsWith("s", StringComparison.OrdinalIgnoreCase))
			{
				ticksPerUnit = TimeSpan.TicksPerSecond;
				number = text[..^1];
			}
			else if (text.EndsWith("m", StringComparison.OrdinalIgnoreCase))
			{
				ticksPerUnit = TimeSpan.TicksPerMinute;
				number = text[..^1];
			}
			else if (text.EndsWith("h", StringComparison.OrdinalIgnoreCase))
			{
				ticksPerUnit = TimeSpan.TicksPerHour;
				number = text[..^1];
			}
			else
			{
				throw Invalid(field, value);
			}

			try
			{
				if (!long.TryParse(
					number,
					NumberStyles.None,
					CultureInfo.InvariantCulture,
					out long amount) ||
					amount <= 0)
				{
					throw Invalid(field, value);
				}
				return TimeSpan.FromTicks(checked(amount * ticksPerUnit));
			}
			catch (OverflowException)
			{
				throw Invalid(field, value);
			}
		}

		internal static long ParseSize(string value, string field)
		{
			string text = value?.Trim();

			if (string.IsNullOrEmpty(text))
			{
				throw Invalid(field, value);
			}

			long multiplier = 1;
			string number = text;
			(string Suffix, long Multiplier)[] units =
			[
				("gb", 1024L * 1024L * 1024L),
				("mb", 1024L * 1024L),
				("kb", 1024L),
				("b", 1L)
			];

			foreach ((string suffix, long unitMultiplier) in units)
			{
				if (text.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
				{
					multiplier = unitMultiplier;
					number = text[..^suffix.Length];
					break;
				}
			}

			try
			{
				if (!long.TryParse(
					number,
					NumberStyles.None,
					CultureInfo.InvariantCulture,
					out long amount) ||
					amount < 0)
				{
					throw Invalid(field, value);
				}
				return checked(amount * multiplier);
			}
			catch (OverflowException)
			{
				throw Invalid(field, value);
			}
		}

		private static AerospikeException Invalid(string field, string value)
		{
			return new AerospikeException(
				ResultCode.PARAMETER_ERROR,
				"Invalid " + field + " value: " + value);
		}
	}
}
