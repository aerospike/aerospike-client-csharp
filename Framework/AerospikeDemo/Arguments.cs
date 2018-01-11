/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections.Generic;
using Aerospike.Client;

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
		internal WritePolicy writePolicy;
		internal Policy policy;
        internal int commandMax;
        internal bool singleBin;
		internal bool hasGeo;
		internal bool hasUdf;
		internal bool hasCDTList;
		internal bool hasCDTMap;

		protected internal Arguments()
		{
			this.writePolicy = new WritePolicy();
			this.policy = new Policy();
		}

		/// <summary>
		/// Some database calls need to know how the server is configured.
		/// </summary>
		protected internal void SetServerSpecific(AerospikeClient client)
		{
			Node node = client.Nodes[0];
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(null, node, featuresFilter, namespaceFilter);

			string features = tokens[featuresFilter];
			hasGeo = false;
			hasUdf = false;
			hasCDTList = false;
			hasCDTMap = false;

			if (features != null)
			{
				string[] list = features.Split(';');

				foreach (string s in list)
				{
					if (s.Equals("geo"))
					{
						hasGeo = true;
					}
					else if (s.Equals("udf"))
					{
						hasUdf = true;
					}
					else if (s.Equals("cdt-list"))
					{
						hasCDTList = true;
					}
					else if (s.Equals("cdt-map"))
					{
						hasCDTMap = true;
					}
				}
			}

			string namespaceTokens = tokens[namespaceFilter];

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
