/* 
 * Copyright 2012-2024 Aerospike, Inc.
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

namespace Aerospike.Client.Proxy
{
	/// <summary>
	/// AerospikeClientFactory generates a new Client of the specified type
	/// with the specified ClientPolicy and sets up the cluster using the provided hosts.
	/// </summary>
	public class AerospikeClientFactory
	{
		/// <summary>
		/// Return an AerospikeClient or AerospikeClientProxy based on isProxy
		/// </summary>
		/// <param name="policy">client configuration policy, use null for defaults</param>
		/// <param name="isProxy">if true, return AerospikeClientProxy otherwise return AerospikeClient</param>
		/// <param name="hosts">array of server hosts that the client can connect</param>
		public static IAerospikeClient CreateClient(ClientPolicy policy, bool isProxy, params Host[] hosts)
		{
			if (isProxy)
			{
				return new AerospikeClientProxy(policy, hosts);
			}
			else
			{
				return new AerospikeClient(policy, hosts);
			}
		}

		/// <summary>
		/// Return an AsyncClient or AsyncClientProxy based on isProxy.
		/// </summary>
		/// <param name="policy">client configuration policy, use null for defaults</param>
		/// <param name="isProxy">if true, return AsyncClientProxy otherwise return AsyncClient</param>
		/// <param name="hosts">array of server hosts that the client can connect</param>
		public static IAsyncClient CreateAsyncClient(AsyncClientPolicy policy, bool isProxy, params Host[] hosts)
		{
			if (isProxy)
			{
				return new AsyncClientProxy(policy, hosts);
			}
			else
			{
				return new AsyncClient(policy, hosts);
			}
		}
	}
}
