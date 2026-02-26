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

namespace Aerospike.Client
{
	/// <summary>
	/// Represents a single metric measurement.
	/// </summary>
	public readonly struct Metric
	{
		/// <summary>
		/// Metric name (e.g., "aerospike_client_command_count", "aerospike_client_node_connections_in_use").
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Metric value.
		/// </summary>
		public double Value { get; }

		/// <summary>
		/// Type of metric (Counter, Gauge, or Histogram).
		/// </summary>
		public MetricType Type { get; }

		/// <summary>
		/// Timestamp when the metric was captured.
		/// </summary>
		public DateTime Timestamp { get; }

		/// <summary>
		/// Labels/tags for the metric (e.g., node name, namespace, bucket).
		/// Uses array for performance - less allocation than dictionary.
		/// </summary>
		public KeyValuePair<string, string>[] Labels { get; }

		/// <summary>
		/// Human-readable description of this metric, suitable for rendering in dashboards.
		/// </summary>
		public string Description { get; }

		/// <summary>
		/// Unit of the metric value (e.g., "By" for bytes, "%" for percentage, "{connection}" for dimensionless counts).
		/// Follows OpenTelemetry/UCUM conventions.
		/// </summary>
		public string Unit { get; }

		/// <summary>
		/// Create a new metric.
		/// </summary>
		public Metric(string name, double value, MetricType type, DateTime timestamp,
			KeyValuePair<string, string>[] labels, string description = null, string unit = null)
		{
			Name = name;
			Value = value;
			Type = type;
			Timestamp = timestamp;
			Labels = labels ?? EmptyLabels;
			Description = description;
			Unit = unit;
		}

		/// <summary>
		/// Create a new metric with current timestamp.
		/// </summary>
		public Metric(string name, double value, MetricType type, KeyValuePair<string, string>[] labels = null,
			string description = null, string unit = null)
			: this(name, value, type, DateTime.UtcNow, labels, description, unit)
		{
		}

		private static readonly KeyValuePair<string, string>[] EmptyLabels = Array.Empty<KeyValuePair<string, string>>();

		public override string ToString()
		{
			if (Labels.Length == 0)
			{
				return $"{Name}={Value}";
			}
			var labelStr = string.Join(",", Labels.Select(kvp => $"{kvp.Key}={kvp.Value}"));
			return $"{Name}{{{labelStr}}}={Value}";
		}
	}

	/// <summary>
	/// Type of metric.
	/// </summary>
	public enum MetricType
	{
		/// <summary>
		/// A cumulative value that only increases (e.g., total commands, total bytes).
		/// </summary>
		Counter,

		/// <summary>
		/// A point-in-time value that can go up or down (e.g., connections in use, CPU percentage).
		/// </summary>
		Gauge,

		/// <summary>
		/// A histogram bucket value (e.g., latency distribution).
		/// </summary>
		Histogram
	}
}
