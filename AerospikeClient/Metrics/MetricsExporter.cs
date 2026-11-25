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
	/// Receives immutable periodic and final client metrics snapshots on the
	/// client's serialized metrics reporter thread. Exporter failures are isolated.
	/// Exporters are owned and disposed by the application that registered them.
	/// </summary>
	public interface IMetricsExporter
	{
		/// <summary>
		/// Export one immutable metrics snapshot.
		/// Implementations should copy data they retain after this method returns.
		/// Calling metrics enable, disable, or client close synchronously from this
		/// callback throws <see cref="AerospikeException"/>. Pulling a snapshot is allowed.
		/// </summary>
		void Export(MetricsSnapshot snapshot);
	}

	/// <summary>
	/// Selects the exclusive metrics delivery path.
	/// </summary>
	public enum MetricsExporterMode
	{
		/// <summary>Collect metrics without periodic export.</summary>
		None,

		/// <summary>Export through the client-owned built-in metrics file writer.</summary>
		File,

		/// <summary>Export through application-owned registered exporters.</summary>
		Custom
	}
}
