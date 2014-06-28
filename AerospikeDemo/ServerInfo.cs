/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ServerInfo : SyncExample
	{
		public ServerInfo(Console console) : base(console)
		{
		}

		/// <summary>
		/// Query server configuration, cluster status and namespace configuration.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Node node = client.Nodes[0];
			GetServerConfig(node, args);
			console.Write("");
			GetNamespaceConfig(node, args);
		}

		/// <summary>
		/// Query server configuration and cluster status.
		/// </summary>
		private void GetServerConfig(Node node, Arguments args)
		{
			console.Write("Server Configuration");
			Dictionary<string, string> map = Info.Request(null, node);

			if (map == null)
			{
				throw new Exception("Failed to get server info: host=" + node);
			}

			foreach (KeyValuePair<string, string> entry in map)
			{
				string key = entry.Key;

				if (key.Equals("statistics") || key.Equals("query-stat"))
				{
					LogNameValueTokens(entry.Value);
				}
				else
				{
					console.Write(key + '=' + entry.Value);
				}
			}
		}

		/// <summary>
		/// Query namespace configuration.
		/// </summary>
		private void GetNamespaceConfig(Node node, Arguments args)
		{
			console.Write("Namespace Configuration");
			string filter = "namespace/" + args.ns;
			string tokens = Info.Request(node, filter);

			if (tokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", 
					node, args.ns));
			}

			LogNameValueTokens(tokens);
		}

		private void LogNameValueTokens(string tokens)
		{
			string[] values = tokens.Split(';');

			foreach (string value in values)
			{
				console.Write(value);
			}
		}
	}
}
