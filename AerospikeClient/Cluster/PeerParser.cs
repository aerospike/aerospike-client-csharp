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
		private string nodeName;
		private string tlsName;
		private readonly int portDefault;
		public readonly int generation;

		public PeerParser(Cluster cluster, Node node, Connection conn, List<Peer> peers)
		{
			this.cluster = cluster;

			string command = (cluster.tlsPolicy != null) ?
				cluster.useServicesAlternate ? "peers-tls-alt" : "peers-tls-std" :
				cluster.useServicesAlternate ? "peers-clear-alt" : "peers-clear-std";

			List<string> stdNames = null;

			if (cluster.useServicesAlternate)
			{
				// The alternate list omits peers that have no alternate address.
				string stdCommand = (cluster.tlsPolicy != null) ? "peers-tls-std" : "peers-clear-std";
				parser = new Info(node, conn, stdCommand, command);
				parser.ParseName(stdCommand);
				stdNames = ParseNodeNames();
				parser.SkipDelimiter('\n');
			}
			else
			{
				parser = new Info(node, conn, command);
			}

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

			if (parser.buffer[parser.offset] != ']')
			{
				ParsePeers(peers);
			}

			if (stdNames != null)
			{
				AddAlternateMissing(peers, stdNames);
			}
		}

		private void ParsePeers(List<Peer> peers)
		{
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

		private List<string> ParseNodeNames()
		{
			List<string> names = new List<string>();
			parser.SkipDelimiter('[');
			int depth = 1;

			while (depth > 0 && parser.offset < parser.length && parser.buffer[parser.offset] != '\n')
			{
				byte b = parser.buffer[parser.offset++];

				if (b == '[')
				{
					if (++depth == 2)
					{
						names.Add(parser.ParseString(','));
					}
				}
				else if (b == ']')
				{
					depth--;
				}
			}

			return names;
		}

		private static void AddAlternateMissing(List<Peer> peers, List<string> stdNames)
		{
			foreach (string name in stdNames)
			{
				bool found = false;

				foreach (Peer peer in peers)
				{
					if (peer.nodeName.Equals(name))
					{
						found = true;
						break;
					}
				}

				if (!found)
				{
					Peer peer = new Peer
					{
						nodeName = name,
						hosts = new List<Host>(0),
						alternateMissing = true
					};
					peers.Add(peer);
				}
			}
		}

		private Peer ParsePeer()
		{
			Peer peer = new Peer();
			parser.Expect('[');
			peer.nodeName = nodeName = parser.ParseString(',');
			parser.offset++;
			peer.tlsName = tlsName = parser.ParseString(',');
			parser.offset++;
			peer.alternateMissing = cluster.useServicesAlternate &&
				parser.offset + 1 < parser.length &&
				parser.buffer[parser.offset + 1] == ']';
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
				parser.offset++;
				return hosts;
			}

			while (true)
			{
				try
				{
					hosts.Add(ParseHost());
				}
				catch (AerospikeException.TranslationMiss tm)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(cluster.context, tm.Message);
					}
				}

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

			if (parser.offset < parser.length)
			{
				byte b = parser.buffer[parser.offset];

				if (b == ':')
				{
					parser.offset++;
					return Translate(host, parser.ParseInt());
				}

				if (b == ',' || b == ']')
				{
					return Translate(host, portDefault);
				}
			}

			string response = parser.GetTruncatedResponse();
			throw new AerospikeException.Parse("Unterminated host in response: " + response);
		}

		private Host Translate(string host, int port)
		{
			return cluster.TranslateAddress(nodeName, tlsName, host, port);
		}
	}
}
