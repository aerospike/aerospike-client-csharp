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

public sealed class ServerInfo : SyncExample
{
	/// <summary>
	/// Query server configuration, cluster status and namespace configuration.
	/// </summary>
	public override void RunExample()
	{
		Node node = client.Nodes[0];
		PrintServerConfig(node);
		Console.WriteLine("");
		PrintNamespaceConfig(node);
	}

	private static void PrintServerConfig(Node node)
	{
		Console.WriteLine("Server Configuration");
		Dictionary<string, string> map = Info.Request(null, node)
			?? throw new Exception($"Failed to get server info: host={node}");

		foreach (KeyValuePair<string, string> entry in map)
		{
			if (entry.Key is "statistics" or "query-stat")
			{
				PrintNameValueTokens(entry.Value);
			}
			else
			{
				Console.WriteLine($"{entry.Key}={entry.Value}");
			}
		}
	}

	private void PrintNamespaceConfig(Node node)
	{
		Console.WriteLine("Namespace Configuration");
		string filter = $"namespace/{ns}";
		string tokens = Info.Request(node, filter)
			?? throw new Exception($"Failed to get namespace info: host={node} namespace={ns}");
		PrintNameValueTokens(tokens);
	}

	private static void PrintNameValueTokens(string tokens)
	{
		foreach (string value in tokens.Split(';'))
		{
			Console.WriteLine(value);
		}
	}
}
