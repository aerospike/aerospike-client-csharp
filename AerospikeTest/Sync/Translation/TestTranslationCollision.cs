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
using MessageList = Aerospike.Test.ConformanceEnv.MessageList;

namespace Aerospike.Test
{
	/// <summary>
	/// Translated address collisions against the two node clusters of the aerospike-client-conformance
	/// collision environment. In the control cluster each node advertises 127.0.0.1 and its own
	/// published host port in its alternate access address. In the collision cluster node B advertises
	/// node A's published port. Every case seeds node A only. The clusters arrive as the environment
	/// variables of its values file:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host the nodes are published on</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// <item><c>CONTROL_NODE_A_NAME</c>, <c>CONTROL_NODE_A_PORT</c>, <c>CONTROL_NODE_B_NAME</c>,
	/// <c>CONTROL_NODE_B_PORT</c>: control cluster</item>
	/// <item><c>COLLISION_NODE_A_NAME</c>, <c>COLLISION_NODE_A_PORT</c>, <c>COLLISION_NODE_B_NAME</c>:
	/// collision cluster</item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestTranslationCollision
	{
		private const int TEND_INTERVAL = 250;
		private const int COLLISION_TENDS = 8;
		private const int JOIN_WAIT_MS = 10000;
		private const int KEY_COUNT = 200;

		// The client logs a collision at warning level when a translator produced the shared address
		// and at info level when the server advertised it unchanged. Capture both.
		private static readonly MessageList messages = new MessageList();

		private static string host;
		private static string ns;
		private static string controlA;
		private static int controlPortA;
		private static string controlB;
		private static int controlPortB;
		private static string collisionA;
		private static int collisionPortA;
		private static string collisionB;

		/// <summary>
		/// Translator that maps every endpoint to one host.
		/// </summary>
		public sealed class SingleAddressTranslator : IAddressTranslator
		{
			private readonly Host target;

			public SingleAddressTranslator(Host target)
			{
				this.target = target;
			}

			public Host Translate(Endpoint advertised)
			{
				return target;
			}
		}

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("ENV_HOST", "ENV_NAMESPACE", "CONTROL_NODE_A_NAME", "CONTROL_NODE_A_PORT",
				"CONTROL_NODE_B_NAME", "CONTROL_NODE_B_PORT", "COLLISION_NODE_A_NAME", "COLLISION_NODE_A_PORT",
				"COLLISION_NODE_B_NAME");

			host = ConformanceEnv.Get("ENV_HOST");
			ns = ConformanceEnv.Get("ENV_NAMESPACE");
			controlA = ConformanceEnv.Get("CONTROL_NODE_A_NAME");
			controlPortA = ConformanceEnv.GetInt("CONTROL_NODE_A_PORT");
			controlB = ConformanceEnv.Get("CONTROL_NODE_B_NAME");
			controlPortB = ConformanceEnv.GetInt("CONTROL_NODE_B_PORT");
			collisionA = ConformanceEnv.Get("COLLISION_NODE_A_NAME");
			collisionPortA = ConformanceEnv.GetInt("COLLISION_NODE_A_PORT");
			collisionB = ConformanceEnv.Get("COLLISION_NODE_B_NAME");

			Log.SetLevel(Log.Level.INFO);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN || level == Log.Level.INFO)
				{
					messages.Add(message);
				}
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			Log.SetCallback(null);
		}

		[TestInitialize]
		public void ClearMessages()
		{
			messages.Clear();
		}

		[TestMethod]
		public void DistinctPortsJoinBothNodes()
		{
			using (AerospikeClient client = new AerospikeClient(NewPolicy(), new Host(host, controlPortA)))
			{
				long deadline = Environment.TickCount64 + JOIN_WAIT_MS;

				while (client.Nodes.Length < 2 && Environment.TickCount64 < deadline)
				{
					Thread.Sleep(TEND_INTERVAL);
				}

				List<string> names = ConformanceEnv.NodeNames(client);
				Console.WriteLine("COLLISION-RESULT control nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names) +
					" collisionWarnings=" + CollisionMessages().Count);
				CollectionAssert.AreEqual(new List<string> {controlA, controlB}, names);
				Assert.AreEqual(0, CollisionMessages().Count, ConformanceEnv.Fmt(CollisionMessages()));
				Assert.AreEqual(controlPortB, client.Cluster.GetNode(controlB).Host.port);
			}
		}

		[TestMethod]
		public void SharedAlternatePortCreatesNoPeer()
		{
			AssertCollides("collision", NewPolicy(), collisionPortA, collisionA, collisionB, "static-map");
		}

		[TestMethod]
		public void CustomTranslatorToNodeACreatesNoPeer()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new SingleAddressTranslator(new Host(host, controlPortA));
			AssertCollides("custom", policy, controlPortA, controlA, controlB, typeof(SingleAddressTranslator).FullName);
		}

		private static ClientPolicy NewPolicy()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.useServicesAlternate = true;
			policy.tendInterval = TEND_INTERVAL;
			return policy;
		}

		private static void AssertCollides(string name, ClientPolicy policy, int portA, string nodeA, string nodeB,
			string translator)
		{
			using (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA)))
			{
				for (int i = 0; i < COLLISION_TENDS; i++)
				{
					Thread.Sleep(TEND_INTERVAL);
					CollectionAssert.AreEqual(new List<string> {nodeA}, ConformanceEnv.NodeNames(client));
				}

				List<string> collisions = CollisionMessages();
				Console.WriteLine("COLLISION-RESULT " + name + " nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(ConformanceEnv.NodeNames(client)) + " collisionWarnings=" + collisions.Count);

				if (collisions.Count == 0)
				{
					Assert.Fail("No translation collision logged for node " + nodeB + ": " +
						ConformanceEnv.Fmt(messages.Snapshot()));
				}

				string message = collisions[0];
				Console.WriteLine("COLLISION-LOG " + name + ' ' + message);

				foreach (string part in new string[] {nodeA, nodeB, host + ':' + portA, translator,
					"Give each node its own reachable address or mapping"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
				Assert.IsTrue(collisions.Count >= 2, "Colliding peer not retried on a later tend: " + ConformanceEnv.Fmt(collisions));
				AssertOwnedKeysWork(name, client, nodeA);
			}
		}

		private static void AssertOwnedKeysWork(string name, AerospikeClient client, string nodeA)
		{
			Cluster cluster = client.Cluster;
			WritePolicy policy = new WritePolicy(client.WritePolicyDefault);
			int ownedByA = 0;
			int notOwned = 0;

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(ns, "translation", i);
				Node node;

				try
				{
					node = Partition.Write(cluster, policy, key).GetNodeWrite(cluster);
				}
				catch (AerospikeException.InvalidNode)
				{
					notOwned++;
					continue;
				}

				Assert.AreEqual(nodeA, node.Name);
				client.Put(policy, key, new Bin("v", i));
				Record record = client.Get(null, key);
				Assert.AreEqual(i, record.GetInt("v"));
				ownedByA++;
			}
			Console.WriteLine("COLLISION-KEYS " + name + " ownedByA=" + ownedByA + " notOwned=" + notOwned);
			Assert.IsTrue(ownedByA > 0);
			Assert.IsTrue(notOwned > 0);
		}

		private static List<string> CollisionMessages()
		{
			return messages.Containing("to the same address");
		}
	}
}
