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
using Aerospike.Client;
using System;
using System.Collections.Generic;

namespace Aerospike.Example
{
	/// <summary>
	/// Configuration data populated from command-line arguments and/or .runsettings,
	/// then enriched with server-specific information after connecting.
	/// </summary>
	public class Arguments
	{
		// Connection parameters (populated by Program.cs)
		public Host[] hosts;
		public int port;
		public string user;
		public string password;
		public string clusterName;
		public string ns;
		public string set;
		public TlsPolicy tlsPolicy;
		public AuthMode authMode;
		public bool useServicesAlternate;
		public int commandMax;

		// Example lists (populated by Program.cs)
		public List<string> syncExamples = [];
		public List<string> asyncExamples = [];

		// Policies (populated after client connection)
		public WritePolicy writePolicy;
		public Policy policy;
		public BatchPolicy batchPolicy;

		// Server capabilities (populated by SetServerSpecific)
		public string binName;
		public bool singleBin;
		public bool enterprise;
		public bool scMode;
		public Version serverVersion;

		public Arguments()
		{
			writePolicy = new WritePolicy();
			policy = new Policy();
			batchPolicy = new BatchPolicy();
		}

		/// <summary>
		/// Query the server after connecting to discover edition, namespace
		/// configuration, and server version.
		/// </summary>
		public void SetServerSpecific(IAerospikeClient client)
		{
			Node node = client.Nodes[0];
			serverVersion = node.serverVersion;

			string editionFilter = serverVersion >= Node.SERVER_VERSION_8_1_1 ? "release" : "edition";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> map = Info.Request(null, node, editionFilter, namespaceFilter);

			string editionToken = map[editionFilter]
				?? throw new Exception($"Failed to get edition: host={node}");
			enterprise = editionToken.Contains("Enterprise");

			string namespaceTokens = map[namespaceFilter]
				?? throw new Exception($"Failed to get namespace info: host={node} namespace={ns}");
			singleBin = ParseBoolean(namespaceTokens, "single-bin");
			scMode = ParseBoolean(namespaceTokens, "strong-consistency");

			binName = singleBin ? "" : "demobin";
		}

		private static bool ParseBoolean(string namespaceTokens, string name)
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

			string value = namespaceTokens[begin..end];
			return Convert.ToBoolean(value);
		}

		public override string ToString()
		{
			return $"Arguments: hosts={Util.ArrayToString(hosts)} port={port} ns={ns} set={set} single-bin={singleBin}";
		}

		public string GetBinName(string name)
		{
			return singleBin ? "" : name;
		}
	}
}
