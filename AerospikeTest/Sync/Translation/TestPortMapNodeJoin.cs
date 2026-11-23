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
using static Aerospike.Test.PortMapCluster;

namespace Aerospike.Test
{
	/// <summary>
	/// A node whose port-map entry exists before the node does joins when it is added to the cluster,
	/// with no client change. Starts node B, so it must run before <see cref="TestPortMap"/> in a
	/// fresh port-map environment.
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestPortMapNodeJoin
	{
		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			Require();
		}

		[TestMethod]
		public void PrefilledEntryJoinsNewNode()
		{
			Assert.IsFalse(NodeBRunning(), "Node B " + NodeB + " is already running");

			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(new Dictionary<string, string>
			{
				{AddressA, EnvHost + ':' + PortA},
				{AddressB, EnvHost + ':' + PortB}
			});

			using (AerospikeClient client = Connect(policy))
			{
				CollectionAssert.AreEqual(new List<string> {NodeA}, NodeNames(client));
				AssertAllPartitionsOwned("n19-before", client);

				Node seed = client.Cluster.GetNode(NodeA);
				string generation = Info.Request(seed, "peers-generation");
				Console.WriteLine("PORT-MAP-RESULT n19-before nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(NodeNames(client)) + " peersGeneration=" + generation);

				Task<string> start = StartNodeB();
				long started = Environment.TickCount64;
				long peersChanged = 0;
				long joined = 0;
				long deadline = started + StartWaitMs;

				while (joined == 0 && Environment.TickCount64 < deadline)
				{
					if (peersChanged == 0 && !Info.Request(seed, "peers-generation").Equals(generation))
					{
						peersChanged = Environment.TickCount64;
					}

					if (client.Nodes.Length == 2)
					{
						joined = Environment.TickCount64;
					}
					Thread.Sleep(10);
				}

				Assert.IsTrue(joined > 0, "Node B did not join within " + StartWaitMs + "ms of being started");
				Assert.IsTrue(peersChanged > 0, "Peers change not observed before node B joined");

				long joinMs = joined - peersChanged;
				Console.WriteLine("PORT-MAP-RESULT n19-join nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(NodeNames(client)) + " startedToPeersChangeMs=" + (peersChanged - started) +
					" peersChangeToJoinMs=" + joinMs + " limitMs=" + (2 * TendInterval) + " tendInterval=" + TendInterval);

				CollectionAssert.AreEqual(new List<string> {NodeA, NodeB}, NodeNames(client));
				Assert.IsTrue(joinMs <= 2 * TendInterval,
					"Node B joined " + joinMs + "ms after the peers change, limit " + (2 * TendInterval) + "ms");

				SortedDictionary<string, int> owners = AssertAllPartitionsOwned("n19-after", client, 2);
				Assert.AreEqual(2, owners.Count, ConformanceEnv.FmtMap(owners));
				start.GetAwaiter().GetResult();
			}
		}
	}
}
