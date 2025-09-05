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
using Aerospike.Client;
using System;

namespace Aerospike.Demo
{
	/// <summary>
	/// Configuration data.
	/// </summary>
	public class Arguments
	{
		internal Host[] hosts;
		internal int port;
		internal string user;
		internal string password;
		internal string clusterName;
		internal string ns;
		internal string set;
		internal string binName;
		internal TlsPolicy tlsPolicy;
		internal AuthMode authMode;
		internal WritePolicy writePolicy;
		internal Policy policy;
		internal BatchPolicy batchPolicy;
		internal int commandMax;
		internal bool singleBin;

		protected internal Arguments()
		{
			this.writePolicy = new WritePolicy();
			this.policy = new Policy();
			this.batchPolicy = new BatchPolicy();
		}

		/// <summary>
		/// Some database calls need to know how the server is configured.
		/// </summary>
		protected internal void SetServerSpecific(IAerospikeClient client)
		{
			Node node = client.Nodes[0];
			string namespaceFilter = "namespace/" + ns;
			string namespaceTokens = Info.Request(null, node, namespaceFilter);

			if (namespaceTokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, ns));
			}

			singleBin = parseBoolean(namespaceTokens, "single-bin");

			binName = singleBin ? "" : "demobin";  // Single bin servers don't need a bin name.
		}

		private static bool parseBoolean(String namespaceTokens, String name)
		{
			string search = name + '=';
			int begin = namespaceTokens.IndexOf(search);

			if (begin < 0)
			{
				return false;
			}

			begin += search.Length;
			int end = namespaceTokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = namespaceTokens.Length;
			}

			string value = namespaceTokens.Substring(begin, end - begin);
			return Convert.ToBoolean(value);
		}

		public override string ToString()
		{
			return "Parameters: hosts=" + Util.ArrayToString(hosts) + " port=" + port + " ns=" + ns + " set=" + set + " single-bin=" + singleBin;
		}

		public virtual string GetBinName(string name)
		{
			// Single bin servers don't need a bin name.
			return singleBin ? "" : name;
		}
	}
}
