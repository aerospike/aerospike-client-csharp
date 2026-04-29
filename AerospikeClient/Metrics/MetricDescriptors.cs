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
	/// Canonical metric names, descriptions, units, and tier classification for all
	/// Aerospike client metrics.
	///
	/// Metrics are classified as either <b>Standard</b> (always collected, negligible
	/// overhead) or <b>Extended</b> (detailed diagnostics, must be explicitly enabled
	/// via <see cref="MetricsPolicy.EnableExtendedMetrics"/>).
	/// </summary>
	public static class MetricDescriptors
	{
		/// <summary>
		/// Static metadata for a single metric.
		/// </summary>
		public readonly struct Descriptor
		{
			public string Name { get; }
			public string Description { get; }
			public string Unit { get; }

			/// <summary>
			/// True when this metric is only collected when extended metrics are enabled.
			/// </summary>
			public bool IsExtended { get; }

			public Descriptor(string name, string description, string unit, bool isExtended = false)
			{
				Name = name;
				Description = description;
				Unit = unit;
				IsExtended = isExtended;
			}
		}

		// ── Standard cluster-level metrics ─────────────────────────────────

		public static readonly Descriptor AsyncThreadsInUse = new(
			"aerospike_client_async_threads_in_use",
			"Async worker threads currently in use",
			"thread");

		public static readonly Descriptor AsyncCompletionPortsInUse = new(
			"aerospike_client_async_completion_ports_in_use",
			"Async I/O completion port threads currently in use",
			"thread");

		public static readonly Descriptor RecoverQueueSize = new(
			"aerospike_client_recover_queue_size",
			"Number of connections in the sync connection recover queue",
			"connection");

		public static readonly Descriptor InvalidNodeCount = new(
			"aerospike_client_invalid_node_count",
			"Count of failed node additions",
			"node");

		public static readonly Descriptor RetryCount = new(
			"aerospike_client_retry_count",
			"Total command retries",
			"retry");

		public static readonly Descriptor DelayQueueTimeoutCount = new(
			"aerospike_client_delay_queue_timeout_count",
			"Async commands that timed out in the delay queue",
			"timeout");

		// ── Extended cluster-level metrics ─────────────────────────────────

		public static readonly Descriptor CpuPercent = new(
			"aerospike_client_cpu_percent",
			"Client process CPU usage percentage",
			"%",
			isExtended: true);

		public static readonly Descriptor MemoryBytes = new(
			"aerospike_client_memory_bytes",
			"Client process memory usage",
			"bytes",
			isExtended: true);

		public static readonly Descriptor CommandCount = new(
			"aerospike_client_command_count",
			"Total commands issued by the client",
			"command",
			isExtended: true);

		// ── Standard node connection metrics ───────────────────────────────

		public static readonly Descriptor ConnectionsInUse = new(
			"aerospike_client_node_connections_in_use",
			"Active connections from the pool",
			"connection");

		public static readonly Descriptor ConnectionsInPool = new(
			"aerospike_client_node_connections_in_pool",
			"Idle connections in the pool",
			"connection");

		public static readonly Descriptor ConnectionsOpened = new(
			"aerospike_client_node_connections_opened",
			"Total connections opened to a node",
			"connection");

		public static readonly Descriptor ConnectionsClosed = new(
			"aerospike_client_node_connections_closed",
			"Total connections closed to a node",
			"connection");

		// ── Extended namespace metrics ─────────────────────────────────────

		public static readonly Descriptor NamespaceErrors = new(
			"aerospike_client_namespace_errors",
			"Command error count per namespace",
			"error",
			isExtended: true);

		public static readonly Descriptor NamespaceTimeouts = new(
			"aerospike_client_namespace_timeouts",
			"Command timeout count per namespace",
			"timeout",
			isExtended: true);

		public static readonly Descriptor NamespaceKeyBusy = new(
			"aerospike_client_namespace_key_busy",
			"Key busy error count per namespace",
			"error",
			isExtended: true);

		public static readonly Descriptor NamespaceBytesIn = new(
			"aerospike_client_namespace_bytes_in",
			"Total bytes read from a namespace",
			"bytes",
			isExtended: true);

		public static readonly Descriptor NamespaceBytesOut = new(
			"aerospike_client_namespace_bytes_out",
			"Total bytes written to a namespace",
			"bytes",
			isExtended: true);

		public static readonly Descriptor LatencyBucket = new(
			"aerospike_client_latency_bucket",
			"Latency distribution bucket count",
			"count",
			isExtended: true);
	}
}
