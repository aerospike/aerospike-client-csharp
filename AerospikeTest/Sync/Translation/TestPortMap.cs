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
using MessageList = Aerospike.Test.ConformanceEnv.MessageList;

namespace Aerospike.Test
{
	/// <summary>
	/// port-map translator against both nodes, each reachable only through its port-map entry.
	/// Runs against the port-map conformance environment. Node B is started when the class starts, so
	/// <see cref="TestPortMapNodeJoin"/> must run before this class in a fresh environment.
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestPortMap
	{
		private const int MISS_TENDS = 5;
		private const int KEY_COUNT = 200;
		private static readonly MessageList warnings = new MessageList();

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			Require();
			StartNodeB().GetAwaiter().GetResult();
			AssertUnreachable(AddressA);
			AssertUnreachable(AddressB);

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
		public void HostPortKeysJoinBothNodes()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(new Dictionary<string, string>
			{
				{AddressA, EnvHost + ':' + PortA},
				{AddressB, EnvHost + ':' + PortB}
			});
			AssertJoins("n2-host-port-keys", policy);
		}

		[TestMethod]
		public void NodeNameKeysJoinBothNodes()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(new Dictionary<string, string>
			{
				{NodeA, EnvHost + ':' + PortA},
				{NodeB, EnvHost + ':' + PortB}
			});
			AssertJoins("node-name-keys", policy);
		}

		[TestMethod]
		public void StrictUnmappedNodeNeverCreated()
		{
			ClientPolicy policy = NewPolicy();
			policy.addressTranslator = new PortMapAddressTranslator(new Dictionary<string, string>
			{
				{AddressA, EnvHost + ':' + PortA}
			});
			policy.translationOnMiss = TranslationMissMode.STRICT;

			using (AerospikeClient client = Connect(policy))
			{
				for (int i = 0; i < MISS_TENDS; i++)
				{
					Thread.Sleep(TendInterval);
					CollectionAssert.AreEqual(new List<string> {NodeA}, NodeNames(client));
				}

				List<string> misses = warnings.Containing("has no mapping for node " + NodeB);

				Console.WriteLine("PORT-MAP-RESULT strict-unmapped nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(NodeNames(client)) + " missWarnings=" + misses.Count);

				if (misses.Count == 0)
				{
					Assert.Fail("No translation miss logged for node " + NodeB + ": " + ConformanceEnv.Fmt(warnings.Snapshot()));
				}

				string message = misses[0];
				Console.WriteLine("PORT-MAP-MISS " + message);

				foreach (string part in new string[] {"Address translator port-map ", NodeB, AddressB, "standard"})
				{
					Assert.IsTrue(message.Contains(part), part + " not in: " + message);
				}
			}
		}

		private static void AssertJoins(string name, ClientPolicy policy)
		{
			using (AerospikeClient client = Connect(policy))
			{
				WaitNodes(client, 2);

				List<string> names = NodeNames(client);
				Console.WriteLine("PORT-MAP-RESULT " + name + " nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names));
				CollectionAssert.AreEqual(new List<string> {NodeA, NodeB}, names);

				SortedDictionary<string, int> owners = AssertAllPartitionsOwned(name, client);
				Assert.AreEqual(2, owners.Count, ConformanceEnv.FmtMap(owners));

				for (int i = 0; i < KEY_COUNT; i++)
				{
					Key key = new Key(Namespace, "portmap", i);
					client.Put(null, key, new Bin("v", i));
					Record record = client.Get(null, key);
					Assert.AreEqual(i, record.GetInt("v"));
				}
				Console.WriteLine("PORT-MAP-KEYS " + name + " written=" + KEY_COUNT + " read=" + KEY_COUNT);
				Assert.AreEqual(0, warnings.Count, ConformanceEnv.Fmt(warnings.Snapshot()));
			}
		}
	}
}
