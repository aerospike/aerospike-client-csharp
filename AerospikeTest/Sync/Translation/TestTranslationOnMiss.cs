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
	/// <see cref="ClientPolicy.translationOnMiss"/> against a two node cluster started by the
	/// aerospike-client-conformance on-miss environment. Each node advertises 127.0.0.1 and its own
	/// published host port in its alternate access address, so node B is reachable only as advertised.
	/// Every case seeds node A only. The cluster arrives as the environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host both nodes are published on</item>
	/// <item><c>NODE_A_NAME</c>, <c>NODE_A_PORT</c>: seed node name and port</item>
	/// <item><c>NODE_B_NAME</c>, <c>NODE_B_PORT</c>: peer node name and port</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestTranslationOnMiss
	{
		private const int TEND_INTERVAL = 250;
		private const int MISS_TENDS = 8;
		private const int JOIN_WAIT_MS = 10000;
		private const int KEY_COUNT = 200;
		private static readonly MessageList warnings = new MessageList();

		private static string host;
		private static string nodeA;
		private static int portA;
		private static string nodeB;
		private static int portB;
		private static string ns;

		/// <summary>
		/// Translator that maps nothing, so every endpoint is a miss.
		/// </summary>
		public sealed class EmptyTranslator : IAddressTranslator
		{
			public Host Translate(Endpoint advertised)
			{
				return null;
			}
		}

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("ENV_HOST", "NODE_A_NAME", "NODE_A_PORT", "NODE_B_NAME", "NODE_B_PORT", "ENV_NAMESPACE");

			host = ConformanceEnv.Get("ENV_HOST");
			nodeA = ConformanceEnv.Get("NODE_A_NAME");
			portA = ConformanceEnv.GetInt("NODE_A_PORT");
			nodeB = ConformanceEnv.Get("NODE_B_NAME");
			portB = ConformanceEnv.GetInt("NODE_B_PORT");
			ns = ConformanceEnv.Get("ENV_NAMESPACE");

			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN)
				{
					warnings.Add(message);
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
		public void ClearWarnings()
		{
			warnings.Clear();
		}

		[TestMethod]
		public void DefaultJoinsBothNodes()
		{
			AssertJoins("default", NewPolicy());
		}

		[TestMethod]
		public void EmptyIpMapStrictDropsPeer()
		{
			ClientPolicy policy = NewPolicy();
			policy.ipMap = new Dictionary<string, string>();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertMisses("ipmap-empty-strict", policy, "static-map");
		}

		[TestMethod]
		public void IdentityIpMapStrictJoinsBothNodes()
		{
			ClientPolicy policy = NewPolicy();
			policy.ipMap = new Dictionary<string, string> {{host, host}};
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertJoins("ipmap-identity-strict", policy);
		}

		[TestMethod]
		public void CustomEmptyStrictDropsPeer()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new EmptyTranslator();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertMisses("custom-empty-strict", policy, typeof(EmptyTranslator).FullName);
		}

		[TestMethod]
		public void CustomEmptyPassThroughJoinsBothNodes()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new EmptyTranslator();
			policy.translationOnMiss = TranslationMissMode.PASS_THROUGH;
			AssertJoins("custom-empty-pass-through", policy, typeof(EmptyTranslator).FullName);
		}

		private static ClientPolicy NewPolicy()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.useServicesAlternate = true;
			policy.tendInterval = TEND_INTERVAL;
			return policy;
		}

		private static void AssertJoins(string name, ClientPolicy policy)
		{
			AssertJoins(name, policy, null);
		}

		private static void AssertJoins(string name, ClientPolicy policy, string missTranslator)
		{
			using (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA)))
			{
				long deadline = Environment.TickCount64 + JOIN_WAIT_MS;

				while (client.Nodes.Length < 2 && Environment.TickCount64 < deadline)
				{
					Thread.Sleep(TEND_INTERVAL);
				}

				List<string> names = ConformanceEnv.NodeNames(client);
				Console.WriteLine("ON-MISS-RESULT " + name + " nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names));
				CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);

				if (missTranslator == null)
				{
					Assert.AreEqual(0, MissWarnings().Count, ConformanceEnv.Fmt(MissWarnings()));
					return;
				}

				List<string> misses = MissWarnings();
				Assert.AreEqual(1, misses.Count, ConformanceEnv.Fmt(misses));

				string message = misses[0];
				Console.WriteLine("ON-MISS-LOG " + name + ' ' + message);

				foreach (string part in new string[] {nodeB, host + ':' + portB, "alternate", missTranslator,
					"Connecting to the advertised address", "pass_through"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
			}
		}

		private static void AssertMisses(string name, ClientPolicy policy, string translator)
		{
			using (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA)))
			{
				for (int i = 0; i < MISS_TENDS; i++)
				{
					Thread.Sleep(TEND_INTERVAL);
					CollectionAssert.AreEqual(new List<string> {nodeA}, ConformanceEnv.NodeNames(client));
				}

				List<string> misses = MissWarnings();
				Console.WriteLine("ON-MISS-RESULT " + name + " nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(ConformanceEnv.NodeNames(client)) + " missWarnings=" + misses.Count);

				if (misses.Count == 0)
				{
					Assert.Fail("No translation miss logged for node " + nodeB + ": " + ConformanceEnv.Fmt(warnings.Snapshot()));
				}

				string message = misses[0];
				Console.WriteLine("ON-MISS-LOG " + name + ' ' + message);

				foreach (string part in new string[] {nodeB, host + ':' + portB, "alternate", translator, "pass_through"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
				Assert.IsTrue(misses.Count >= 2, "Missed peer not retried on a later tend: " + ConformanceEnv.Fmt(misses));
				AssertOwnedKeysWork(name, client);
			}
		}

		private static void AssertOwnedKeysWork(string name, AerospikeClient client)
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
			Console.WriteLine("ON-MISS-KEYS " + name + " ownedByA=" + ownedByA + " notOwned=" + notOwned);
			Assert.IsTrue(ownedByA > 0);
			Assert.IsTrue(notOwned > 0);
		}

		private static List<string> MissWarnings()
		{
			return warnings.Containing("has no mapping for node " + nodeB);
		}
	}
}
