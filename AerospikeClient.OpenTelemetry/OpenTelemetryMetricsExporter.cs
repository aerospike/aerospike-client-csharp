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

using System.Collections.Concurrent;
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
	/// <remarks>
	/// The Meter lifecycle is always owned by the application via MeterProvider.
	/// This exporter does not dispose the Meter; call MeterProvider.Dispose() when done.
	/// </remarks>
	/// <example>
	/// <code>
	/// // Setup MeterProvider - it owns the Meter lifecycle
	/// var meter = new Meter("Aerospike.Client", "1.0.0");
	/// using var meterProvider = Sdk.CreateMeterProviderBuilder()
	///     .AddMeter(meter.Name)
	///     .AddOtlpExporter()
	///     .Build();
	/// 
	/// var exporter = new OpenTelemetryMetricsExporter(meter);
	/// 
	/// var policy = new MetricsPolicy { Interval = 30 };
	/// policy.AddExporter(exporter);
	/// client.EnableMetrics(policy);
	/// 
	/// // On shutdown: client.Close() flushes final metrics automatically.
	/// // MeterProvider.Dispose() cleans up the Meter.
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

		// Lock-free instrument registration tracking
		private readonly ConcurrentDictionary<string, byte> registeredInstruments = new();
		private readonly object registrationLock = new();

		// Latest metrics grouped by name for O(1) callback lookup.
		// Volatile reference swap: the dictionary is fully built before publishing,
		// so readers see a consistent snapshot without locking.
		private volatile Dictionary<string, List<Metric>> latestMetricsByName = new();

		/// <summary>
		/// Create a new OpenTelemetry metrics exporter using the provided meter.
		/// The caller is responsible for the Meter lifecycle via MeterProvider.
		/// </summary>
		/// <param name="meter">The meter to use for creating instruments.</param>
		public OpenTelemetryMetricsExporter(Meter meter)
		{
			this.meter = meter ?? throw new ArgumentNullException(nameof(meter));
		}

		/// <summary>
		/// Export metrics. Dynamically creates OTel instruments for any new metric names encountered.
		/// </summary>
		public void Export(IReadOnlyList<Metric> metrics)
		{
			// Group metrics by name for O(1) lookup in observable callbacks
			var grouped = new Dictionary<string, List<Metric>>();

			foreach (var metric in metrics)
			{
				EnsureInstrumentExists(metric);

				if (!grouped.TryGetValue(metric.Name, out var list))
				{
					list = new List<Metric>();
					grouped[metric.Name] = list;
				}
				list.Add(metric);
			}

			// Atomic reference swap - readers see a fully-constructed snapshot
			latestMetricsByName = grouped;
		}

		/// <summary>
		/// Dispose of the exporter. Clears internal state but does not dispose the Meter;
		/// the Meter lifecycle is owned by the application via MeterProvider.
		/// </summary>
		public void Dispose()
		{
			latestMetricsByName = new Dictionary<string, List<Metric>>();
			registeredInstruments.Clear();
		}

		/// <summary>
		/// Ensures an OTel instrument exists for this metric. Creates it on first encounter.
		/// Uses a lock-free fast path for the common case (instrument already registered).
		/// </summary>
		private void EnsureInstrumentExists(Metric metric)
		{
			if (registeredInstruments.ContainsKey(metric.Name))
			{
				return;
			}

			lock (registrationLock)
			{
				if (!registeredInstruments.TryAdd(metric.Name, 0))
				{
					return;
				}

				string metricName = metric.Name;
				string description = metric.Description ?? $"Aerospike metric: {metricName}";
				string unit = metric.Unit;

				switch (metric.Type)
				{
					case MetricType.Counter:
						meter.CreateObservableCounter(
							metricName,
							() => GetMeasurementsLong(metricName),
							unit: unit,
							description: description);
						break;

					case MetricType.Gauge:
						meter.CreateObservableGauge(
							metricName,
							() => GetMeasurementsDouble(metricName),
							unit: unit,
							description: description);
						break;

					case MetricType.Histogram:
						meter.CreateObservableCounter(
							metricName,
							() => GetMeasurementsLong(metricName),
							unit: unit,
							description: description);
						break;
				}
			}
		}

		private IEnumerable<Measurement<double>> GetMeasurementsDouble(string metricName)
		{
			var byName = latestMetricsByName;
			if (byName.TryGetValue(metricName, out var list))
			{
				foreach (var m in list)
				{
					yield return new Measurement<double>(m.Value, ToTagList(m.Labels));
				}
			}
		}

		private IEnumerable<Measurement<long>> GetMeasurementsLong(string metricName)
		{
			var byName = latestMetricsByName;
			if (byName.TryGetValue(metricName, out var list))
			{
				foreach (var m in list)
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
