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
	/// Interface for exporting metrics to external systems.
	/// Implementations can export metrics to files, OpenTelemetry, Prometheus, etc.
	/// Exporters are responsible for their own initialization and cleanup.
	/// </summary>
	public interface IMetricsExporter
	{
		/// <summary>
		/// Export a batch of metrics. Called periodically based on the configured interval.
		/// </summary>
		/// <param name="metrics">Collection of metrics to export</param>
		void Export(IReadOnlyList<Metric> metrics);
	}

	/// <summary>
	/// Optional async interface for metrics exporters that perform I/O operations.
	/// If an exporter implements this interface, the async method will be preferred
	/// over the synchronous Export method for better scalability.
	/// </summary>
	/// <example>
	/// <code>
	/// public class HttpMetricsExporter : IMetricsExporter, IAsyncMetricsExporter
	/// {
	///     public void Export(IReadOnlyList&lt;Metric&gt; metrics)
	///     {
	///         // Fallback sync implementation
	///         ExportAsync(metrics, CancellationToken.None).GetAwaiter().GetResult();
	///     }
	///     
	///     public async Task ExportAsync(IReadOnlyList&lt;Metric&gt; metrics, CancellationToken cancellationToken)
	///     {
	///         // Non-blocking HTTP call
	///         await httpClient.PostAsync(endpoint, CreatePayload(metrics), cancellationToken);
	///     }
	/// }
	/// </code>
	/// </example>
	public interface IAsyncMetricsExporter : IMetricsExporter
	{
		/// <summary>
		/// Asynchronously export a batch of metrics. Preferred over Export() when available.
		/// </summary>
		/// <param name="metrics">Collection of metrics to export</param>
		/// <param name="cancellationToken">Cancellation token for the export operation</param>
		/// <returns>Task representing the async export operation</returns>
		Task ExportAsync(IReadOnlyList<Metric> metrics, CancellationToken cancellationToken);
	}
}
