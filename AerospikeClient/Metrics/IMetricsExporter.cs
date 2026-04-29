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
		/// Export a point-in-time metrics snapshot. Called periodically based on the configured interval.
		/// </summary>
		/// <param name="snapshot">Structured snapshot of all client metrics</param>
		void Export(MetricsSnapshot snapshot);
	}
}
