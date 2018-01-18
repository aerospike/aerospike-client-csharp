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

namespace Aerospike.Client
{
	/// <summary>
	/// Parse node's peers.
	/// </summary>
	public sealed class PeerParser
	{
		private readonly Cluster cluster;
		private readonly Info parser;
		private string tlsName;
		private readonly int portDefault;
		public readonly int generation;

		public PeerParser(Cluster cluster, Connection conn, List<Peer> peers)
		{
			this.cluster = cluster;

			string command = (cluster.tlsPolicy != null) ?
				cluster.useServicesAlternate ? "peers-tls-alt" : "peers-tls-std" : 
				cluster.useServicesAlternate ? "peers-clear-alt" : "peers-clear-std";

			parser = new Info(conn, command);

			if (parser.length == 0)
			{
				throw new AerospikeException.Parse(command + " response is empty");
			}

			parser.SkipToValue();
			generation = parser.ParseInt();
			parser.Expect(',');
			portDefault = parser.ParseInt();
			parser.Expect(',');
			parser.Expect('[');

			// Reset peers
			peers.Clear();

			if (parser.buffer[parser.offset] == ']')
			{
				return;
			}

			while (true)
			{
				Peer peer = ParsePeer();
				peers.Add(peer);

				if (parser.offset < parser.length && parser.buffer[parser.offset] == ',')
				{
					parser.offset++;
				}
				else
				{
					break;
				}
			} 
		}

		private Peer ParsePeer()
		{
			Peer peer = new Peer();
			parser.Expect('[');
			peer.nodeName = parser.ParseString(',');
			parser.offset++;
			peer.tlsName = tlsName = parser.ParseString(',');
			parser.offset++;
			peer.hosts = ParseHosts();
			parser.Expect(']');
			return peer;
		}

		private List<Host> ParseHosts()
		{
			List<Host> hosts = new List<Host>(4);
			parser.Expect('[');

			if (parser.buffer[parser.offset] == ']')
			{
				return hosts;
			}

			while (true)
			{
				Host host = ParseHost();
				hosts.Add(host);

				if (parser.buffer[parser.offset] == ']')
				{
					parser.offset++;
					return hosts;
				}
				parser.offset++;
			}
		}

		private Host ParseHost()
		{
			string host;

			if (parser.buffer[parser.offset] == '[')
			{
				// IPV6 addresses can start with bracket.
				parser.offset++;
				host = parser.ParseString(']');
				parser.offset++;
			}
			else
			{
				host = parser.ParseString(':', ',', ']');
			}

			if (cluster.ipMap != null)
			{
				string alternativeHost;
				
				if (cluster.ipMap.TryGetValue(host, out alternativeHost))
				{
					host = alternativeHost;
				}
			}

			if (parser.offset < parser.length)
			{
				byte b = parser.buffer[parser.offset];

				if (b == ':')
				{
					parser.offset++;
					int port = parser.ParseInt();
					return new Host(host, tlsName, port);
				}

				if (b == ',' || b == ']')
				{
					return new Host(host, tlsName, portDefault);
				}
			}

			string response = parser.GetTruncatedResponse();
			throw new AerospikeException.Parse("Unterminated host in response: " + response);
		}
	}
}
