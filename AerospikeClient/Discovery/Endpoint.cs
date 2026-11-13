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
	/// Server node address as advertised by the cluster.
	/// </summary>
	public sealed class Endpoint
	{
		/// <summary>
		/// Info command list the endpoint was returned by.
		/// </summary>
		public enum SourceList
		{
			/// <summary>
			/// Standard list: peers-clear-std, peers-tls-std, service-clear-std, service-tls-std.
			/// </summary>
			STANDARD,

			/// <summary>
			/// Alternate list: peers-clear-alt, peers-tls-alt, service-clear-alt, service-tls-alt.
			/// Used when <see cref="ClientPolicy.useServicesAlternate"/> is true.
			/// </summary>
			ALTERNATE
		}

		/// <summary>
		/// Name of the node that advertised the address.
		/// </summary>
		public readonly string nodeName;

		/// <summary>
		/// TLS certificate name of the node. May be null.
		/// </summary>
		public readonly string tlsName;

		/// <summary>
		/// Advertised host name or IP address.
		/// </summary>
		public readonly string host;

		/// <summary>
		/// Advertised port.
		/// </summary>
		public readonly int port;

		/// <summary>
		/// Info command list the address came from.
		/// </summary>
		public readonly SourceList sourceList;

		/// <summary>
		/// Initialize advertised endpoint.
		/// </summary>
		public Endpoint(string nodeName, string tlsName, string host, int port, SourceList sourceList)
		{
			this.nodeName = nodeName;
			this.tlsName = tlsName;
			this.host = host;
			this.port = port;
			this.sourceList = sourceList;
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			return nodeName + ' ' + host + ':' + port + ' ' + sourceList;
		}
	}
}
