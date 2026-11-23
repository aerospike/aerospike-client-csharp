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
using Driver = Aerospike.L5Driver.L5Driver;

namespace Aerospike.Test
{
	/// <summary>
	/// The L5 driver's port-map shape against two stub nodes: the seed advertises its peer at an
	/// address the client has no route to, and the port-map, keyed by node name or by the advertised
	/// <c>HOST:PORT</c>, maps it to the peer's listener. Needs no server.
	/// </summary>
	[TestClass]
	public class TestL5DriverPortMap
	{
		private const string Peers = "1,3000,[[BB9B,,[10.255.0.2]]]";

		[TestMethod]
		public void NodeNameKeyRoutesPeer()
		{
			using (StubInfoNode a = new StubInfoNode("BB9A"))
			using (StubInfoNode b = new StubInfoNode("BB9B"))
			{
				a.Values["peers-clear-std"] = Peers;
				AssertPeerAt(a, b, "BB9B=127.0.0.1:" + b.Port);
			}
		}

		[TestMethod]
		public void AdvertisedAddressKeyRoutesPeer()
		{
			using (StubInfoNode a = new StubInfoNode("BB9A"))
			using (StubInfoNode b = new StubInfoNode("BB9B"))
			{
				a.Values["peers-clear-std"] = Peers;
				AssertPeerAt(a, b, "10.255.0.2:3000=127.0.0.1:" + b.Port);
			}
		}

		private static void AssertPeerAt(StubInfoNode a, StubInfoNode b, string portMap)
		{
			Driver.Options options = new Driver.Options(new[]
			{
				"--seed", "127.0.0.1:" + a.Port, "--shape", "port-map",
				"--port-map", portMap, "--tend-interval", "250", "--timeout", "1000",
				"--namespace", "test", "--set", "l5"
			}, null);

			using (AerospikeClient client = new AerospikeClient(options.Policy(), options.Seed))
			{
				Node peer = Await(client, "BB9B");
				Assert.AreEqual(new Host("127.0.0.1", b.Port), peer.Host);
				Assert.IsTrue(b.Commands().Contains("node"), string.Join(",", b.Commands()));
			}
		}

		internal static Node Await(AerospikeClient client, string name)
		{
			long deadline = Environment.TickCount64 + 10000;

			while (Environment.TickCount64 < deadline)
			{
				foreach (Node node in client.Nodes)
				{
					if (node.Name.Equals(name))
					{
						return node;
					}
				}

				Thread.Sleep(20);
			}

			Assert.Fail("client never held " + name + "; nodes " + string.Join(",",
				Array.ConvertAll(client.Nodes, n => n.Name)));
			return null;
		}
	}
}
