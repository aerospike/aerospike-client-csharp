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

namespace Aerospike.Example;

/// <summary>
/// Configuration data populated from command-line arguments and/or .runsettings,
/// then enriched with server-specific information after connecting.
/// </summary>
/// <remarks>
/// Field naming mirrors the Aerospike client's public field convention so the
/// snippets in this project read naturally next to client API surface.
/// </remarks>
public sealed class Arguments
{
	// Connection parameters (populated by Program.cs).
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

	// Example lists (populated by Program.cs).
	public List<string> syncExamples = [];
	public List<string> asyncExamples = [];
	public string reportTrxPath;

	// Policies (populated after client connection).
	public WritePolicy writePolicy;
	public Policy policy;
	public BatchPolicy batchPolicy;

	// Server capabilities (populated by SetServerSpecific).
	public bool enterprise;
	public bool scMode;
	public Version serverVersion;

	/// <summary>
	/// Query the server after connecting to discover edition, namespace
	/// configuration, and server version.
	/// </summary>
	public void SetServerSpecific(IAerospikeClient client)
	{
		Node node = client.Nodes[0];
		serverVersion = node.serverVersion;

		string editionFilter = serverVersion >= Node.SERVER_VERSION_8_1_1 ? "release" : "edition";
		string namespaceFilter = $"namespace/{ns}";
		Dictionary<string, string> map = Info.Request(null, node, editionFilter, namespaceFilter);

		string editionToken = map[editionFilter]
			?? throw new Exception($"Failed to get edition: host={node}");
		enterprise = editionToken.Contains("Enterprise");

		string namespaceTokens = map[namespaceFilter]
			?? throw new Exception($"Failed to get namespace info: host={node} namespace={ns}");
		scMode = ParseFlag(namespaceTokens, "strong-consistency");
	}

	private static bool ParseFlag(string namespaceTokens, string name)
	{
		string search = $"{name}=";
		int begin = namespaceTokens.IndexOf(search, StringComparison.Ordinal);

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

		ReadOnlySpan<char> value = namespaceTokens.AsSpan(begin, end - begin);
		return bool.TryParse(value, out bool result) && result;
	}

	public override string ToString()
		=> $"Arguments: hosts={Util.ArrayToString(hosts)} port={port} ns={ns} set={set}";
}
