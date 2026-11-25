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
using System.Net.Sockets;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Two node cluster of the aerospike-client-conformance port-map environment. Both nodes advertise
	/// only their container address on port 3000, which the host cannot reach, and each is published on
	/// its own host port. Node B's container is created but not started. The cluster arrives as the
	/// environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host both nodes are published on</item>
	/// <item><c>NODE_A_NAME</c>, <c>NODE_A_ADVERTISED_HOST</c>:<c>NODE_A_ADVERTISED_PORT</c>,
	/// <c>NODE_A_PORT</c>: node A name, advertised address, published port</item>
	/// <item><c>NODE_B_NAME</c>, <c>NODE_B_ADVERTISED_HOST</c>:<c>NODE_B_ADVERTISED_PORT</c>,
	/// <c>NODE_B_PORT</c>: node B name, advertised address, published port</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// </list>
	/// Node B is started with the <c>start-node-b</c> verb.
	/// </summary>
	internal static class PortMapCluster
	{
		internal const int TendInterval = 1000;
		internal const int JoinWaitMs = 30000;
		internal const int StartWaitMs = 120000;

		internal static string EnvHost => ConformanceEnv.Get("ENV_HOST");
		internal static string NodeA => ConformanceEnv.Get("NODE_A_NAME");
		internal static string AddressA => ConformanceEnv.Get("NODE_A_ADVERTISED_HOST") + ':' + ConformanceEnv.Get("NODE_A_ADVERTISED_PORT");
		internal static int PortA => ConformanceEnv.GetInt("NODE_A_PORT");
		internal static string NodeB => ConformanceEnv.Get("NODE_B_NAME");
		internal static string AddressB => ConformanceEnv.Get("NODE_B_ADVERTISED_HOST") + ':' + ConformanceEnv.Get("NODE_B_ADVERTISED_PORT");
		internal static int PortB => ConformanceEnv.GetInt("NODE_B_PORT");
		internal static string Namespace => ConformanceEnv.Get("ENV_NAMESPACE");

		/// <summary>
		/// Mark the calling test class inconclusive when the port-map environment is not loaded.
		/// </summary>
		internal static void Require()
		{
			ConformanceEnv.Require("ENV_HOST", "ENV_CTL", "ENV_NAMESPACE", "NODE_A_NAME", "NODE_A_ADVERTISED_HOST",
				"NODE_A_ADVERTISED_PORT", "NODE_A_PORT", "NODE_B_NAME", "NODE_B_ADVERTISED_HOST",
				"NODE_B_ADVERTISED_PORT", "NODE_B_PORT");
		}

		internal static ClientPolicy NewPolicy()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.tendInterval = TendInterval;
			policy.timeout = 1000;
			return policy;
		}

		internal static AerospikeClient Connect(ClientPolicy policy)
		{
			return new AerospikeClient(policy, new Host(EnvHost, PortA));
		}

		internal static List<string> NodeNames(AerospikeClient client)
		{
			return ConformanceEnv.NodeNames(client);
		}

		internal static void WaitNodes(AerospikeClient client, int count)
		{
			long deadline = Environment.TickCount64 + JoinWaitMs;

			while (client.Nodes.Length < count && Environment.TickCount64 < deadline)
			{
				Thread.Sleep(100);
			}
		}

		/// <summary>
		/// Wait until every partition of the namespace has a master in the client's partition map and
		/// return the number of partitions each node owns.
		/// </summary>
		internal static SortedDictionary<string, int> AssertAllPartitionsOwned(string name, AerospikeClient client)
		{
			return AssertAllPartitionsOwned(name, client, 1);
		}

		/// <summary>
		/// Wait until every partition of the namespace has a master in the client's partition map and at
		/// least <paramref name="ownerCount"/> nodes own partitions, and return the number of partitions
		/// each node owns.
		/// </summary>
		internal static SortedDictionary<string, int> AssertAllPartitionsOwned(string name, AerospikeClient client, int ownerCount)
		{
			long deadline = Environment.TickCount64 + JoinWaitMs;
			SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);
			string ns = Namespace;
			int unowned;

			do
			{
				unowned = ConformanceEnv.Owners(client, ns, owners);

				if (unowned == 0 && owners.Count >= ownerCount)
				{
					break;
				}
				Thread.Sleep(100);
			}
			while (Environment.TickCount64 < deadline);

			Console.WriteLine("PORT-MAP-PARTITIONS " + name + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
				" owners=" + ConformanceEnv.FmtMap(owners));
			Assert.AreEqual(0, unowned);
			return owners;
		}

		internal static void AssertUnreachable(string address)
		{
			int index = address.LastIndexOf(':');
			string h = address.Substring(0, index);
			int port = int.Parse(address.Substring(index + 1));
			bool reachable = false;

			try
			{
				using (TcpClient tcp = new TcpClient())
				{
					reachable = tcp.ConnectAsync(h, port).Wait(2000) && tcp.Connected;
				}
			}
			catch (Exception expected)
			{
				Console.WriteLine("PORT-MAP-UNREACHABLE " + address + ' ' + expected.Message);
			}

			Assert.IsFalse(reachable, "Advertised address " + address + " is reachable from the host");
		}

		internal static bool NodeBRunning()
		{
			return ConformanceEnv.Control("node-b-state").Equals("running");
		}

		/// <summary>
		/// Start node B in the background. The <c>start-node-b</c> verb returns once the two node cluster is
		/// stable, so a caller can watch node B join while it runs.
		/// </summary>
		internal static Task<string> StartNodeB()
		{
			return Task.Factory.StartNew(() => ConformanceEnv.Control("start-node-b"), TaskCreationOptions.LongRunning);
		}
	}
}
