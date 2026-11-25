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
	/// <see cref="PassThroughAddressTranslator"/> against a two node cluster started by the
	/// aerospike-client-conformance pass-through environment. Each node advertises 127.0.0.1 and its own
	/// published host port in its alternate access address: one host name, a port per node. Every case
	/// seeds node A only. The cluster arrives as the environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host both nodes are published on</item>
	/// <item><c>NODE_A_NAME</c>, <c>NODE_A_PORT</c>: seed node name and port</item>
	/// <item><c>NODE_B_NAME</c>, <c>NODE_B_PORT</c>: peer node name and port</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestPassThroughTranslation
	{
		private const int TEND_INTERVAL = 250;
		private const int JOIN_WAIT_MS = 10000;
		private const int KEY_COUNT = 200;
		private static readonly MessageList warnings = new MessageList();

		private static string host;
		private static string nodeA;
		private static int portA;
		private static string nodeB;
		private static int portB;
		private static string ns;

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
		public void DefaultOnMissJoinsBothNodes()
		{
			AssertJoins("default", NewPolicy());
		}

		[TestMethod]
		public void StrictJoinsBothNodes()
		{
			ClientPolicy policy = NewPolicy();
			policy.translationOnMiss = TranslationMissMode.STRICT;
			AssertJoins("strict", policy);
		}

		private static ClientPolicy NewPolicy()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.useServicesAlternate = true;
			policy.addressTranslator = PassThroughAddressTranslator.Instance;
			policy.tendInterval = TEND_INTERVAL;
			return policy;
		}

		private static void AssertJoins(string name, ClientPolicy policy)
		{
			using (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA)))
			{
				SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);
				long deadline = Environment.TickCount64 + JOIN_WAIT_MS;

				while ((client.Nodes.Length < 2 || ConformanceEnv.Owners(client, ns, owners) > 0) &&
					Environment.TickCount64 < deadline)
				{
					Thread.Sleep(TEND_INTERVAL);
				}

				SortedDictionary<string, string> hosts = ConformanceEnv.NodeHosts(client);
				Console.WriteLine("PASS-THROUGH-RESULT " + name + " nodes=" + hosts.Count + ' ' + ConformanceEnv.FmtMap(hosts));

				SortedDictionary<string, string> expected = new SortedDictionary<string, string>(StringComparer.Ordinal)
				{
					{nodeA, host + ':' + portA},
					{nodeB, host + ':' + portB}
				};
				CollectionAssert.AreEqual(expected, hosts, ConformanceEnv.FmtMap(hosts));

				int unowned = ConformanceEnv.Owners(client, ns, owners);
				Console.WriteLine("PASS-THROUGH-PARTITIONS " + name + " partitions=" + Node.PARTITIONS +
					" unowned=" + unowned + " owners=" + ConformanceEnv.FmtMap(owners));
				Assert.AreEqual(0, unowned);
				Assert.AreEqual(2, owners.Count);
				Assert.IsTrue(owners[nodeA] > 0);
				Assert.IsTrue(owners[nodeB] > 0);

				AssertKeysWork(name, client);
				Console.WriteLine("PASS-THROUGH-WARNINGS " + name + ' ' + ConformanceEnv.Fmt(warnings.Snapshot()));
				Assert.AreEqual(0, warnings.Count, ConformanceEnv.Fmt(warnings.Snapshot()));
			}
		}

		private static void AssertKeysWork(string name, AerospikeClient client)
		{
			WritePolicy policy = new WritePolicy(client.WritePolicyDefault);
			int onA = 0;
			int onB = 0;

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(ns, "pass-through", i);
				Node node = Partition.Write(client.Cluster, policy, key).GetNodeWrite(client.Cluster);

				client.Put(policy, key, new Bin("v", i));
				Record record = client.Get(null, key);
				Assert.AreEqual(i, record.GetInt("v"));

				if (node.Name.Equals(nodeA))
				{
					onA++;
				}
				else
				{
					onB++;
				}
			}
			Console.WriteLine("PASS-THROUGH-KEYS " + name + " onA=" + onA + " onB=" + onB);
			Assert.IsTrue(onA > 0);
			Assert.IsTrue(onB > 0);
		}
	}
}
