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

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Aerospike.Client.OpenTelemetry
{
	/// <summary>
	/// OpenTelemetry-compatible metrics exporter for Aerospike client metrics.
	/// 
	/// This exporter dynamically creates OTel instruments based on the metrics received.
	/// When new metrics are added to the Aerospike client, they are automatically
	/// exported without any changes to this exporter.
	/// </summary>
	/// <example>
	/// <code>
	/// // Option 1: Let exporter create its own meter (simple)
	/// var exporter = new OpenTelemetryMetricsExporter();
	/// 
	/// // Option 2: Pass your own meter (for integration with existing infrastructure)
	/// var meter = new Meter("MyApp.Aerospike", "1.0.0");
	/// var exporter = new OpenTelemetryMetricsExporter(meter);
	/// 
	/// // Configure OpenTelemetry to collect from the meter
	/// var meterProvider = Sdk.CreateMeterProviderBuilder()
	///     .AddMeter("Aerospike.Client")  // or your custom meter name
	///     .AddOtlpExporter()
	///     .Build();
	/// 
	/// var policy = new MetricsPolicy { Interval = 30 };
	/// policy.AddExporter(exporter);
	/// client.EnableMetrics(policy);
	/// </code>
	/// </example>
	public class OpenTelemetryMetricsExporter : IMetricsExporter, IDisposable
	{
		/// <summary>
		/// The default meter name used when no custom meter is provided.
		/// Configure your MeterProvider to listen for this meter.
		/// </summary>
		public const string DefaultMeterName = "Aerospike.Client";

		private readonly Meter meter;
		private readonly bool ownsMeter;

		// Track which instruments have been created
		private readonly HashSet<string> registeredInstruments = new();
		private readonly object registrationLock = new();

		// Latest metrics snapshot for observable instruments
		private IReadOnlyList<Metric> latestMetrics = Array.Empty<Metric>();
		private readonly object metricsLock = new();

		/// <summary>
		/// Create a new OpenTelemetry metrics exporter with the default meter name.
		/// </summary>
		public OpenTelemetryMetricsExporter() 
			: this(new Meter(DefaultMeterName), true)
		{
		}

		/// <summary>
		/// Create a new OpenTelemetry metrics exporter with a custom meter name.
		/// </summary>
		/// <param name="meterName">Custom meter name.</param>
		/// <param name="version">Optional meter version.</param>
		public OpenTelemetryMetricsExporter(string meterName, string version = null)
			: this(new Meter(meterName, version), true)
		{
		}

		/// <summary>
		/// Create a new OpenTelemetry metrics exporter using an existing meter.
		/// Use this to integrate with your application's existing observability infrastructure.
		/// </summary>
		/// <param name="meter">The meter to use for creating instruments. The exporter does not own this meter.</param>
		public OpenTelemetryMetricsExporter(Meter meter)
			: this(meter, false)
		{
		}

		private OpenTelemetryMetricsExporter(Meter meter, bool ownsMeter)
		{
			this.meter = meter ?? throw new ArgumentNullException(nameof(meter));
			this.ownsMeter = ownsMeter;
		}

		/// <summary>
		/// Export metrics. Dynamically creates OTel instruments for any new metric names encountered.
		/// </summary>
		public void Export(IReadOnlyList<Metric> metrics)
		{
			// Ensure instruments exist for all metric names (creates on first encounter)
			foreach (var metric in metrics)
			{
				EnsureInstrumentExists(metric);
			}

			// Store for callbacks to read
			lock (metricsLock)
			{
				latestMetrics = metrics;
			}
		}

		/// <summary>
		/// Dispose of the exporter. Only disposes the meter if it was created by this exporter.
		/// </summary>
		public void Dispose()
		{
			if (ownsMeter)
			{
				meter?.Dispose();
			}
		}

		/// <summary>
		/// Ensures an OTel instrument exists for this metric. Creates it on first encounter.
		/// </summary>
		private void EnsureInstrumentExists(Metric metric)
		{
			lock (registrationLock)
			{
				// Already registered - nothing to do
				if (!registeredInstruments.Add(metric.Name))
				{
					return;
				}

				// First time seeing this metric name - create instrument
				string otelName = ToOtelName(metric.Name);
				string metricName = metric.Name; // Capture for closure

				switch (metric.Type)
				{
					case MetricType.Counter:
						meter.CreateObservableCounter(
							otelName,
							() => GetMeasurementsLong(metricName),
							description: $"Aerospike metric: {metricName}");
						break;

					case MetricType.Gauge:
						meter.CreateObservableGauge(
							otelName,
							() => GetMeasurementsDouble(metricName),
							description: $"Aerospike metric: {metricName}");
						break;

					case MetricType.Histogram:
						// Histogram buckets are exported as counters (cumulative bucket counts)
						meter.CreateObservableCounter(
							otelName,
							() => GetMeasurementsLong(metricName),
							description: $"Aerospike histogram bucket: {metricName}");
						break;
				}
			}
		}

		/// <summary>
		/// Convert Aerospike metric name to OTel-friendly name.
		/// Removes _total suffix (OTel adds it for counters automatically in some backends).
		/// </summary>
		private static string ToOtelName(string metricName)
		{
			// Remove _total suffix as OTel conventions handle this
			if (metricName.EndsWith("_total"))
			{
				return metricName[..^6]; // Remove "_total"
			}
			return metricName;
		}

		private IEnumerable<Measurement<double>> GetMeasurementsDouble(string metricName)
		{
			IReadOnlyList<Metric> metrics;
			lock (metricsLock)
			{
				metrics = latestMetrics;
			}

			foreach (var m in metrics)
			{
				if (m.Name == metricName)
				{
					yield return new Measurement<double>(m.Value, ToTagList(m.Labels));
				}
			}
		}

		private IEnumerable<Measurement<long>> GetMeasurementsLong(string metricName)
		{
			IReadOnlyList<Metric> metrics;
			lock (metricsLock)
			{
				metrics = latestMetrics;
			}

			foreach (var m in metrics)
			{
				if (m.Name == metricName)
				{
					yield return new Measurement<long>((long)m.Value, ToTagList(m.Labels));
				}
			}
		}

		private static TagList ToTagList(KeyValuePair<string, string>[] labels)
		{
			var tagList = new TagList();
			foreach (var label in labels)
			{
				tagList.Add(label.Key, label.Value);
			}
			return tagList;
		}
	}
}
