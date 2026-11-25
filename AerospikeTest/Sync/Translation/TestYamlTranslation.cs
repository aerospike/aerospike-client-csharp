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
	/// Address translation configured only through the YAML config file, against the two clusters of
	/// the aerospike-client-conformance yaml environment. Each case constructs the client with a default
	/// <see cref="ClientPolicy"/> and seeds node A only. The environment's config path is passed to the
	/// client as the file URI in <c>AEROSPIKE_CLIENT_CONFIG_URL</c>.
	/// <list type="bullet">
	/// <item>pass-through: each node advertises 127.0.0.1 and its own published host port in its
	/// alternate access address. The YAML sets use_services_alternate and translation.provider
	/// pass-through.</item>
	/// <item>port-map: each node advertises only its container address. The YAML sets
	/// translation.provider port-map with a port_map to the published host ports.</item>
	/// </list>
	/// The clusters arrive as the environment variables of its values file, &lt;CASE&gt; being
	/// PASS_THROUGH or PORT_MAP:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host every node is published on</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// <item>&lt;CASE&gt;_CLIENT_CONFIG: YAML config file</item>
	/// <item>&lt;CASE&gt;_NODE_A_NAME, &lt;CASE&gt;_NODE_A_PORT: seed node name and port</item>
	/// <item>&lt;CASE&gt;_NODE_B_NAME, &lt;CASE&gt;_NODE_B_PORT: peer node name and port</item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestYamlTranslation
	{
		private const int TEND_INTERVAL = 250;
		private const int JOIN_WAIT_MS = 10000;
		private const int KEY_COUNT = 200;
		private static readonly MessageList warnings = new MessageList();

		private static string host;
		private static string ns;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("ENV_HOST", "ENV_NAMESPACE",
				"PASS_THROUGH_CLIENT_CONFIG", "PASS_THROUGH_NODE_A_NAME", "PASS_THROUGH_NODE_A_PORT",
				"PASS_THROUGH_NODE_B_NAME", "PASS_THROUGH_NODE_B_PORT",
				"PORT_MAP_CLIENT_CONFIG", "PORT_MAP_NODE_A_NAME", "PORT_MAP_NODE_A_PORT",
				"PORT_MAP_NODE_B_NAME", "PORT_MAP_NODE_B_PORT");
			ConformanceEnv.RequireNoConfig();

			host = ConformanceEnv.Get("ENV_HOST");
			ns = ConformanceEnv.Get("ENV_NAMESPACE");

			Log.SetLevel(Log.Level.INFO);
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

		[TestCleanup]
		public void ClearConfig()
		{
			Environment.SetEnvironmentVariable(ConformanceEnv.ConfigUrlVariable, null);
		}

		[TestMethod]
		public void PassThroughFromYaml()
		{
			AssertJoins("passThrough", "PASS_THROUGH", policy =>
			{
				Assert.IsTrue(policy.useServicesAlternate);
				Assert.AreSame(PassThroughAddressTranslator.Instance, policy.addressTranslator);
				Assert.AreEqual(TranslationMissMode.PASS_THROUGH, policy.translationOnMiss);
			});
		}

		[TestMethod]
		public void PortMapFromYaml()
		{
			AssertJoins("portMap", "PORT_MAP", policy =>
			{
				Assert.IsFalse(policy.useServicesAlternate);
				Assert.IsInstanceOfType<PortMapAddressTranslator>(policy.addressTranslator);
				Assert.AreEqual(TranslationMissMode.STRICT, policy.translationOnMiss);
			});
		}

		private static void AssertJoins(string name, string prefix, Action<ClientPolicy> check)
		{
			string config = ConformanceEnv.Get(prefix + "_CLIENT_CONFIG");
			string nodeA = ConformanceEnv.Get(prefix + "_NODE_A_NAME");
			int portA = ConformanceEnv.GetInt(prefix + "_NODE_A_PORT");
			string nodeB = ConformanceEnv.Get(prefix + "_NODE_B_NAME");
			int portB = ConformanceEnv.GetInt(prefix + "_NODE_B_PORT");

			ConformanceEnv.SetConfig(config);

			ClientPolicy codePolicy = new ClientPolicy();
			Assert.IsNull(codePolicy.addressTranslator);

			using (AerospikeClient client = new AerospikeClient(codePolicy, new Host(host, portA)))
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.AreEqual(TEND_INTERVAL, policy.tendInterval);
				check(policy);

				SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);
				long deadline = Environment.TickCount64 + JOIN_WAIT_MS;

				while ((client.Nodes.Length < 2 || ConformanceEnv.Owners(client, ns, owners) > 0) &&
					Environment.TickCount64 < deadline)
				{
					Thread.Sleep(TEND_INTERVAL);
				}

				SortedDictionary<string, string> hosts = ConformanceEnv.NodeHosts(client);
				Console.WriteLine("YAML-RESULT " + name + " translator=" + policy.addressTranslator.GetType().Name +
					" useServicesAlternate=" + policy.useServicesAlternate + " onMiss=" + policy.translationOnMiss +
					" nodes=" + hosts.Count + ' ' + ConformanceEnv.FmtMap(hosts));

				SortedDictionary<string, string> expected = new SortedDictionary<string, string>(StringComparer.Ordinal)
				{
					{nodeA, host + ':' + portA},
					{nodeB, host + ':' + portB}
				};
				CollectionAssert.AreEqual(expected, hosts, ConformanceEnv.FmtMap(hosts));

				int unowned = ConformanceEnv.Owners(client, ns, owners);
				Console.WriteLine("YAML-PARTITIONS " + name + " partitions=" + Node.PARTITIONS +
					" unowned=" + unowned + " owners=" + ConformanceEnv.FmtMap(owners));
				Assert.AreEqual(0, unowned);
				Assert.AreEqual(2, owners.Count);
				Assert.IsTrue(owners[nodeA] > 0);
				Assert.IsTrue(owners[nodeB] > 0);

				AssertKeysWork(name, client, nodeA);
				Console.WriteLine("YAML-WARNINGS " + name + ' ' + ConformanceEnv.Fmt(warnings.Snapshot()));
				Assert.AreEqual(0, warnings.Count, ConformanceEnv.Fmt(warnings.Snapshot()));
			}
		}

		private static void AssertKeysWork(string name, AerospikeClient client, string nodeA)
		{
			WritePolicy policy = new WritePolicy(client.WritePolicyDefault);
			int onA = 0;
			int onB = 0;

			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(ns, "yaml-translation", i);
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
			Console.WriteLine("YAML-KEYS " + name + " onA=" + onA + " onB=" + onB);
			Assert.IsTrue(onA > 0);
			Assert.IsTrue(onB > 0);
		}
	}
}
