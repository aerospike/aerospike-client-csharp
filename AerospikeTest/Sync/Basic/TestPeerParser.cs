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
using System.Net;
using System.Net.Sockets;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestPeerParser
	{
		private const string PeersResponse =
			"peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1:3001,10.0.0.9]],[BB9C,,[10.0.0.3]],[BB9B,,[10.0.0.2]]]\n";

		[TestMethod]
		public void StrictMissDropsOnlyMissedHost()
		{
			List<Peer> peers = Parse(TranslationMissMode.STRICT);

			Assert.AreEqual(3, peers.Count);
			Assert.AreEqual("BB9A", peers[0].nodeName);
			AssertHosts([new Host("192.168.1.1", 3001)], peers[0].hosts);
			Assert.AreEqual("BB9C", peers[1].nodeName);
			Assert.AreEqual(0, peers[1].hosts.Count);
			Assert.AreEqual("BB9B", peers[2].nodeName);
			AssertHosts([new Host("192.168.1.2", 3000)], peers[2].hosts);
		}

		[TestMethod]
		public void PassThroughMissKeepsAdvertisedHost()
		{
			List<Peer> peers = Parse(TranslationMissMode.PASS_THROUGH);

			Assert.AreEqual(3, peers.Count);
			AssertHosts(
				[new Host("192.168.1.1", 3001), new Host("10.0.0.9", 3000)],
				peers[0].hosts);
			AssertHosts([new Host("10.0.0.3", 3000)], peers[1].hosts);
			AssertHosts([new Host("192.168.1.2", 3000)], peers[2].hosts);
		}

		private static List<Peer> Parse(TranslationMissMode mode)
		{
			Dictionary<string, string> ipMap = new Dictionary<string, string>
			{
				["10.0.0.1"] = "192.168.1.1",
				["10.0.0.2"] = "192.168.1.2"
			};
			ClientPolicy policy = new ClientPolicy
			{
				failIfNotConnected = false,
				timeout = 100,
				ipMap = ipMap,
				translationOnMiss = mode
			};

			using TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			int port = ((IPEndPoint)listener.LocalEndpoint).Port;
			Task replyTask = Task.Run(() => Reply(listener));

			using AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1));
			Connection conn = new Connection(new IPEndPoint(IPAddress.Loopback, port), 5000, null, null);
			List<Peer> peers = new List<Peer>();

			try
			{
				PeerParser parser = new PeerParser(client.Cluster, null, conn, peers);
				Assert.AreEqual(5, parser.generation);
			}
			finally
			{
				conn.Close();
				replyTask.GetAwaiter().GetResult();
			}

			return peers;
		}

		private static void Reply(TcpListener listener)
		{
			using Socket socket = listener.AcceptSocket();
			byte[] header = new byte[8];
			ReadFully(socket, header);
			long size = ByteUtil.BytesToLong(header, 0) & 0xFFFFFFFFFFFFL;
			byte[] request = new byte[(int)size];
			ReadFully(socket, request);

			byte[] body = Encoding.UTF8.GetBytes(PeersResponse);
			byte[] response = new byte[8 + body.Length];
			ulong proto = unchecked((ulong)(long)body.Length | (2L << 56) | (1L << 48));
			ByteUtil.LongToBytes(proto, response, 0);
			Buffer.BlockCopy(body, 0, response, 8, body.Length);
			socket.Send(response);
		}

		private static void ReadFully(Socket socket, byte[] buffer)
		{
			int offset = 0;

			while (offset < buffer.Length)
			{
				int read = socket.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None);

				if (read <= 0)
				{
					throw new EndOfStreamException();
				}

				offset += read;
			}
		}

		private static void AssertHosts(Host[] expected, List<Host> actual)
		{
			Assert.AreEqual(expected.Length, actual.Count);

			for (int i = 0; i < expected.Length; i++)
			{
				Assert.AreEqual(expected[i].name, actual[i].name, "index " + i);
				Assert.AreEqual(expected[i].port, actual[i].port, "index " + i);
			}
		}
	}
}
