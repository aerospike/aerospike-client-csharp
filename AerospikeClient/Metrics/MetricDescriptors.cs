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
	/// Pre-defined metric descriptors for all Aerospike client metrics.
	/// Each descriptor holds the static metadata (name, description, unit, type) for a metric,
	/// and provides a factory method to create <see cref="Metric"/> instances with that metadata.
	/// </summary>
	public static class MetricDescriptors
	{
		/// <summary>
		/// Holds static metadata for a single metric and creates instances with that metadata.
		/// </summary>
		public readonly struct Descriptor
		{
			public string Name { get; }
			public string Description { get; }
			public string Unit { get; }
			public MetricType Type { get; }

			public Descriptor(string name, string description, string unit, MetricType type)
			{
				Name = name;
				Description = description;
				Unit = unit;
				Type = type;
			}

			public Metric Create(double value, DateTime timestamp, KeyValuePair<string, string>[] labels)
			{
				return new Metric(Name, value, Type, timestamp, labels, Description, Unit);
			}
		}

		// Cluster-level gauges
		public static readonly Descriptor CpuPercent = new(
			"aerospike_client_cpu_percent",
			"Client process CPU usage percentage",
			"%", MetricType.Gauge);

		public static readonly Descriptor MemoryBytes = new(
			"aerospike_client_memory_bytes",
			"Client process memory usage",
			"By", MetricType.Gauge);

		public static readonly Descriptor RecoverQueueSize = new(
			"aerospike_client_recover_queue_size",
			"Number of commands in the recover queue",
			"{item}", MetricType.Gauge);

		public static readonly Descriptor AsyncThreadsInUse = new(
			"aerospike_client_async_threads_in_use",
			"Async worker threads currently in use",
			"{thread}", MetricType.Gauge);

		public static readonly Descriptor AsyncCompletionPortsInUse = new(
			"aerospike_client_async_completion_ports_in_use",
			"Async I/O completion port threads currently in use",
			"{thread}", MetricType.Gauge);

		// Cluster-level counters
		public static readonly Descriptor CommandCount = new(
			"aerospike_client_command_count",
			"Total number of commands executed",
			"{command}", MetricType.Counter);

		public static readonly Descriptor RetryCount = new(
			"aerospike_client_retry_count",
			"Total number of command retries",
			"{retry}", MetricType.Counter);

		public static readonly Descriptor DelayQueueTimeoutCount = new(
			"aerospike_client_delay_queue_timeout_count",
			"Total number of delay queue timeouts",
			"{timeout}", MetricType.Counter);

		public static readonly Descriptor InvalidNodeCount = new(
			"aerospike_client_invalid_node_count",
			"Total number of invalid nodes encountered",
			"{node}", MetricType.Counter);

		// Node connection metrics
		public static readonly Descriptor ConnectionsInUse = new(
			"aerospike_client_node_connections_in_use",
			"Number of active connections to a node",
			"{connection}", MetricType.Gauge);

		public static readonly Descriptor ConnectionsInPool = new(
			"aerospike_client_node_connections_in_pool",
			"Number of idle connections in the pool for a node",
			"{connection}", MetricType.Gauge);

		public static readonly Descriptor ConnectionsOpened = new(
			"aerospike_client_node_connections_opened",
			"Total connections opened to a node",
			"{connection}", MetricType.Counter);

		public static readonly Descriptor ConnectionsClosed = new(
			"aerospike_client_node_connections_closed",
			"Total connections closed to a node",
			"{connection}", MetricType.Counter);

		// Namespace metrics
		public static readonly Descriptor NamespaceErrors = new(
			"aerospike_client_namespace_errors",
			"Total errors for a namespace",
			"{error}", MetricType.Counter);

		public static readonly Descriptor NamespaceTimeouts = new(
			"aerospike_client_namespace_timeouts",
			"Total timeouts for a namespace",
			"{timeout}", MetricType.Counter);

		public static readonly Descriptor NamespaceKeyBusy = new(
			"aerospike_client_namespace_key_busy",
			"Total key busy errors for a namespace",
			"{error}", MetricType.Counter);

		public static readonly Descriptor NamespaceBytesIn = new(
			"aerospike_client_namespace_bytes_in",
			"Total bytes received from a namespace",
			"By", MetricType.Counter);

		public static readonly Descriptor NamespaceBytesOut = new(
			"aerospike_client_namespace_bytes_out",
			"Total bytes sent to a namespace",
			"By", MetricType.Counter);

		// Latency histogram
		public static readonly Descriptor LatencyBucket = new(
			"aerospike_client_latency_bucket",
			"Latency distribution bucket count",
			"{count}", MetricType.Histogram);
	}
}
