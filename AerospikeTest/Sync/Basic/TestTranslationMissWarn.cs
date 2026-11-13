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
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Aerospike.Test
{
	/// <summary>
	/// Pass-through translation misses warn once per node and advertised address when an
	/// address translator is set.
	/// </summary>
	[TestClass]
	public class TestTranslationMissWarn
	{
		private const string Std =
			"peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1]],[BB9B,,[10.0.0.2:3001]]]\n";
		private const string Alt =
			"peers-clear-alt\t5,3000,[[BB9A,,[203.0.113.1:30001]],[BB9B,,[203.0.113.2:30002]]]\n";

		private static readonly Dictionary<string, string> StdPortMap = new Dictionary<string, string>
		{
			["10.0.0.1:3000"] = "lb.example.com:31001",
			["10.0.0.2:3001"] = "lb.example.com:31002"
		};

		[TestMethod]
		public void PassThroughMissOnWrongSourceListWarnsOncePerNode()
		{
			ClientPolicy policy = Policy(true, new PortMapAddressTranslator(StdPortMap));

			using AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1));
			List<string> warnings = new List<string>();
			List<Peer> peers = Parse(client, Std + Alt, warnings);

			AssertHosts([new Host("203.0.113.1", 30001)], peers[0].hosts);
			AssertHosts([new Host("203.0.113.2", 30002)], peers[1].hosts);
			Assert.AreEqual(2, warnings.Count);
			AssertContains(warnings[0], "port-map", "BB9A", "203.0.113.1:30001", "alternate", "pass_through");
			AssertContains(warnings[1], "port-map", "BB9B", "203.0.113.2:30002", "alternate", "pass_through");

			List<string> again = new List<string>();
			Parse(client, Std + Alt, again);
			Assert.AreEqual(0, again.Count);

			List<string> moved = new List<string>();
			Parse(client, Std + Alt.Replace("203.0.113.2:30002", "203.0.113.2:30003"), moved);
			Assert.AreEqual(1, moved.Count);
			AssertContains(moved[0], "BB9B", "203.0.113.2:30003");
		}

		[TestMethod]
		public void DefaultTranslatorMissDoesNotWarn()
		{
			ClientPolicy policy = Policy(true, null);

			using AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1));
			List<string> warnings = new List<string>();
			List<Peer> peers = Parse(client, Std + Alt, warnings);

			AssertHosts([new Host("203.0.113.1", 30001)], peers[0].hosts);
			Assert.AreEqual(0, warnings.Count);
		}

		[TestMethod]
		public void StrictMissOnWrongSourceListWarnsAndDropsHosts()
		{
			ClientPolicy policy = Policy(true, new PortMapAddressTranslator(StdPortMap));
			policy.translationOnMiss = TranslationMissMode.STRICT;
			List<string> warnings = new List<string>();
			int testThreadId = Environment.CurrentManagedThreadId;

			using AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1));
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN && Environment.CurrentManagedThreadId == testThreadId)
				{
					warnings.Add(message);
				}
			});

			try
			{
				List<Peer> peers = ParsePeers(client, Std + Alt);
				Assert.AreEqual(2, peers.Count);
				Assert.AreEqual(0, peers[0].hosts.Count);
				Assert.AreEqual(0, peers[1].hosts.Count);
				Assert.AreEqual(2, warnings.Count);
				AssertContains(warnings[0], "port-map", "BB9A", "203.0.113.1:30001", "alternate");
				AssertContains(warnings[1], "port-map", "BB9B", "203.0.113.2:30002", "alternate");
			}
			finally
			{
				Log.SetCallback(null);
				Log.SetLevel(Log.Level.INFO);
			}
		}

		private static ClientPolicy Policy(bool useServicesAlternate, IAddressTranslator translator)
		{
			return new ClientPolicy
			{
				failIfNotConnected = false,
				timeout = 100,
				useServicesAlternate = useServicesAlternate,
				addressTranslator = translator
			};
		}

		private static List<Peer> Parse(AerospikeClient client, string reply, List<string> warnings)
		{
			int testThreadId = Environment.CurrentManagedThreadId;
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN && Environment.CurrentManagedThreadId == testThreadId)
				{
					warnings.Add(message);
				}
			});

			try
			{
				return ParsePeers(client, reply);
			}
			finally
			{
				Log.SetCallback(null);
				Log.SetLevel(Log.Level.INFO);
			}
		}

		private static List<Peer> ParsePeers(AerospikeClient client, string reply)
		{
			using TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			int port = ((IPEndPoint)listener.LocalEndpoint).Port;
			Task replyTask = Task.Run(() => Reply(listener, reply));

			Connection conn = new Connection(new IPEndPoint(IPAddress.Loopback, port), 5000, null, null);
			List<Peer> peers = new List<Peer>();

			try
			{
				_ = new PeerParser(client.Cluster, null, conn, peers);
			}
			finally
			{
				conn.Close();
				replyTask.GetAwaiter().GetResult();
			}

			return peers;
		}

		private static void Reply(TcpListener listener, string bodyText)
		{
			using Socket socket = listener.AcceptSocket();
			byte[] header = new byte[8];
			ReadFully(socket, header);
			long size = ByteUtil.BytesToLong(header, 0) & 0xFFFFFFFFFFFFL;
			byte[] request = new byte[(int)size];
			ReadFully(socket, request);

			byte[] body = Encoding.UTF8.GetBytes(bodyText);
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

		private static void AssertContains(string message, params string[] parts)
		{
			foreach (string part in parts)
			{
				StringAssert.Contains(message, part);
			}
		}
	}
}
