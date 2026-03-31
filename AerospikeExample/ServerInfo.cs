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

public class ServerInfo(Console console) : SyncExample(console)
{

	/// <summary>
	/// Query server configuration, cluster status and namespace configuration.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		Node node = client.Nodes[0];
		GetServerConfig(node, args);
		Console.Write("");
		GetNamespaceConfig(node, args);
	}

	/// <summary>
	/// Query server configuration and cluster status.
	/// </summary>
	private static void GetServerConfig(Node node, Arguments args)
	{
		Console.Write("Server Configuration");
		var map = Info.Request(null, node) ?? throw new Exception("Failed to get server info: host=" + node);
		foreach (KeyValuePair<string, string> entry in map)
		{
			string key = entry.Key;

			if (key.Equals("statistics") || key.Equals("query-stat"))
			{
				LogNameValueTokens(entry.Value);
			}
			else
			{
				Console.Write(key + '=' + entry.Value);
			}
		}
	}

	/// <summary>
	/// Query namespace configuration.
	/// </summary>
	private static void GetNamespaceConfig(Node node, Arguments args)
	{
		Console.Write("Namespace Configuration");
		string filter = "namespace/" + args.ns;
		string tokens = Info.Request(node, filter) ?? throw new Exception($"Failed to get namespace info: host={node} namespace={args.ns}");
		LogNameValueTokens(tokens);
	}

	private static void LogNameValueTokens(string tokens)
	{
		string[] values = tokens.Split(';');

		foreach (string value in values)
		{
			Console.Write(value);
		}
	}
}
