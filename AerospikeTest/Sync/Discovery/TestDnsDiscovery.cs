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
	/// <see cref="DnsSeedCandidateProvider"/> against the two node cluster and CoreDNS server of the
	/// aerospike-client-conformance dns environment. The client gets no static seed: every seed comes
	/// from DNS. Node A is reachable on 127.0.0.1 and node B on another host address, both on the same
	/// port, and each advertises that address as its alternate access address. The provider's debug log
	/// line names the resolved names and the number of hosts, and the hosts themselves are read from the
	/// seed list the client holds. Every case states its execution mode. The environment arrives as the
	/// environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>DNS_SERVER</c>: DNS server address, ip:port</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// <item><c>NODE_A_PORT</c>: port node A and node B are reachable on</item>
	/// <item><c>NODE_A_NAME</c>, <c>NODE_A_HOST</c>, <c>NODE_B_NAME</c>, <c>NODE_B_HOST</c>: node names and
	/// reachable addresses</item>
	/// <item><c>NODE_C_HOST</c>, <c>NODE_C_PORT</c>: address and port of the standalone node C</item>
	/// <item><c>CLIENT_CONFIG</c>: YAML config file selecting provider dns, passed to the client as the
	/// file URI in <c>AEROSPIKE_CLIENT_CONFIG_URL</c></item>
	/// </list>
	/// The outage case stops and starts the DNS server with the <c>stop-dns</c> and <c>start-dns</c>
	/// verbs, and the changed record case points _change._tcp at node C and back with
	/// <c>set-change-srv</c>.
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestDnsDiscovery
	{
		private const string SEEDS_NAME = "seeds.dns.test";
		private const string CHANGE_NAME = "_change._tcp.dns.test";
		private const string RESOLVED = "DNS seed names ";
		private const string FAILED = "Seed candidate provider dns failed: ";
		private const string TEND_THREAD = "tend";
		private const string REFRESH_THREAD = "aerospike-seed-refresh";
		private const int REFRESH_INTERVAL = 1000;
		private const int WAIT_MS = 20000;
		private const int KEY_COUNT = 100;

		private static readonly MessageList messages = new MessageList();
		private static readonly MessageList resolvedThreads = new MessageList();

		private static string server;
		private static string ns;
		private static int port;
		private static string nodeA;
		private static string hostA;
		private static string nodeB;
		private static string hostB;
		private static string hostC;
		private static int portC;
		private static string config;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("DNS_SERVER", "ENV_NAMESPACE", "ENV_CTL", "NODE_A_PORT", "NODE_A_NAME", "NODE_A_HOST",
				"NODE_B_NAME", "NODE_B_HOST", "NODE_C_HOST", "NODE_C_PORT", "CLIENT_CONFIG");
			ConformanceEnv.RequireNoConfig();

			server = ConformanceEnv.Get("DNS_SERVER");
			ns = ConformanceEnv.Get("ENV_NAMESPACE");
			port = ConformanceEnv.GetInt("NODE_A_PORT");
			nodeA = ConformanceEnv.Get("NODE_A_NAME");
			hostA = ConformanceEnv.Get("NODE_A_HOST");
			nodeB = ConformanceEnv.Get("NODE_B_NAME");
			hostB = ConformanceEnv.Get("NODE_B_HOST");
			hostC = ConformanceEnv.Get("NODE_C_HOST");
			portC = ConformanceEnv.GetInt("NODE_C_PORT");
			config = ConformanceEnv.Get("CLIENT_CONFIG");

			Log.SetLevel(Log.Level.DEBUG);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN || level == Log.Level.ERROR ||
					message.StartsWith(RESOLVED, StringComparison.Ordinal))
				{
					if (message.StartsWith(RESOLVED, StringComparison.Ordinal))
					{
						resolvedThreads.Add(Thread.CurrentThread.Name ?? "unnamed");
					}
					messages.Add(message);
					Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
				}
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
			resolvedThreads.Clear();
		}

		[TestCleanup]
		public void ClearConfig()
		{
			Environment.SetEnvironmentVariable(ConformanceEnv.ConfigUrlVariable, null);
		}

		[TestMethod]
		public void Case1ARecordsJoinBothNodes()
		{
			ClientPolicy policy = NewPolicy(DnsSeedCandidateProvider.CreateBuilder()
				.Names(SEEDS_NAME).Port(port).Servers(server).Build());

			using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
			{
				string seeds = AssertJoins("a-record", client);
				Assert.AreEqual(ASeedLine(), seeds);
				CollectionAssert.AreEqual(ASeeds(), SeedStrings(client));
			}
		}

		[TestMethod]
		public void Case4DnsOutageKeepsClientRunning()
		{
			ClientPolicy policy = NewPolicy(DnsSeedCandidateProvider.CreateBuilder()
				.Names(SEEDS_NAME).Port(port).Servers(server).Build());

			using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
			{
				AssertJoins("outage-before", client);
				messages.Clear();
				Control("stop-dns");

				try
				{
					List<string> failures = Await(() =>
					{
						List<string> list = messages.Snapshot()
							.Where(m => m.StartsWith(FAILED, StringComparison.Ordinal)).ToList();
						return list.Count >= 3 ? list : null;
					});

					foreach (string failure in failures)
					{
						Console.WriteLine("DNS-DISCOVERY-WARN " + failure.Replace('\n', ' '));
						Assert.IsTrue(failure.Contains("DNS seed discovery failed for every name: " + SEEDS_NAME + ": "), failure);
					}

					List<string> names = ConformanceEnv.NodeNames(client);
					Console.WriteLine("DNS-DISCOVERY-RESULT outage nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names) +
						" refreshFailures=" + failures.Count + " connected=" + client.Connected);
					CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
					Assert.IsFalse(messages.Snapshot().Any(m => m.StartsWith("Seed candidate provider exceeded tend deadline",
						StringComparison.Ordinal)));
					AssertKeysWork("outage", client);
				}
				finally
				{
					Control("start-dns");
				}
			}
		}

		[TestMethod]
		public void Case5YamlARecordsJoinBothNodes()
		{
			ConformanceEnv.SetConfig(config);

			using (AerospikeClient client = new AerospikeClient(new ClientPolicy(), Array.Empty<Host>()))
			{
				Assert.IsNotNull(ConfigTestHelpers.GetConfigProvider(client));
				DnsSeedCandidateProvider provider =
					(DnsSeedCandidateProvider)ConfigTestHelpers.GetClientPolicy(client).seedCandidateProvider;
				Console.WriteLine("DNS-DISCOVERY-RESULT yaml provider names=" + ConformanceEnv.Fmt(provider.Names) +
					" recordType=" + provider.Type + " port=" + provider.Port + " servers=" +
					ConformanceEnv.Fmt(provider.Servers));

				string seeds = AssertJoins("yaml", client);
				Assert.AreEqual(ASeedLine(), seeds);
				CollectionAssert.AreEqual(ASeeds(), SeedStrings(client));
			}
		}

		[TestMethod]
		public void Case6ThreadRefreshPicksUpChangedRecord()
		{
			AssertRefreshPicksUpChangedRecord(DiscoveryExecutionMode.THREAD, REFRESH_THREAD);
		}

		[TestMethod]
		public void Case7TimeoutAboveRefreshTimeoutFailsConstruction()
		{
			ClientPolicy policy = NewPolicy(DnsSeedCandidateProvider.CreateBuilder()
				.Names(SEEDS_NAME).Port(port).Servers(server).Timeout(300).Build());
			policy.discoveryRefreshTimeoutMs = 200;

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => new AerospikeClient(policy, Array.Empty<Host>()).Close());
			Console.WriteLine("DNS-DISCOVERY-RESULT timeout " + ae.Message + " resolved=" + resolvedThreads.Count);
			Assert.IsTrue(ae.Message.Contains(
				"DNS seed provider timeout 300 must be less or equal to the discovery refresh timeout 200"), ae.Message);
			Assert.AreEqual(0, resolvedThreads.Count);
		}

		[TestMethod]
		public void Case8TendRefreshPicksUpChangedRecord()
		{
			AssertRefreshPicksUpChangedRecord(DiscoveryExecutionMode.TEND, TEND_THREAD);
		}

		private static void AssertRefreshPicksUpChangedRecord(DiscoveryExecutionMode mode, string thread)
		{
			ClientPolicy policy = NewPolicy(DnsSeedCandidateProvider.CreateBuilder().Names(CHANGE_NAME)
				.RecordType(DnsSeedCandidateProvider.RecordType.SRV).Servers(server).Build());
			policy.discoveryExecutionMode = mode;
			Host nodeC = new Host(hostC, portC);

			using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
			{
				AssertJoins("change-before", client);
				Assert.IsFalse(ClusterInternals.Seeds(client).Contains(nodeC));
				int from = resolvedThreads.Count;
				Control("set-change-srv", "node-c");

				try
				{
					Await(() => ClusterInternals.Seeds(client).Contains(nodeC) ? "ok" : null);
					List<string> all = resolvedThreads.Snapshot();
					HashSet<string> threads = new HashSet<string>(all.GetRange(from, all.Count - from));
					string changed = messages.Snapshot().LastOrDefault(m => m.StartsWith(RESOLVED, StringComparison.Ordinal));
					List<string> names = ConformanceEnv.NodeNames(client);
					Console.WriteLine("DNS-DISCOVERY-RESULT change mode=" + mode + " '" + changed + "' seeds=" +
						ConformanceEnv.Fmt(ClusterInternals.Seeds(client)) + " resolvedOn=" + ConformanceEnv.Fmt(threads) +
						" nodes=" + ConformanceEnv.Fmt(names));
					Assert.IsTrue(threads.SetEquals(new string[] {thread}), ConformanceEnv.Fmt(threads));
					CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
					AssertKeysWork("change", client);
				}
				finally
				{
					Control("set-change-srv", "node-a", "node-b");
					DnsSeedCandidateProvider check = DnsSeedCandidateProvider.CreateBuilder().Names(CHANGE_NAME)
						.RecordType(DnsSeedCandidateProvider.RecordType.SRV).Servers(server).Build();
					HashSet<Host> restored = new HashSet<Host> {new Host(hostA, port), new Host(hostB, port)};
					Await(() => Resolves(check, restored) ? "ok" : null);
				}
			}
		}

		private static bool Resolves(DnsSeedCandidateProvider provider, HashSet<Host> expected)
		{
			try
			{
				return new HashSet<Host>(provider.RefreshSeedCandidates(
					new SeedCandidateContext(null, Array.Empty<Host>(), 0))).SetEquals(expected);
			}
			catch (AerospikeException)
			{
				// The record is mid reload; try again.
				return false;
			}
		}

		private static ClientPolicy NewPolicy(DnsSeedCandidateProvider provider)
		{
			ClientPolicy policy = new ClientPolicy();
			policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
			policy.useServicesAlternate = true;
			policy.discoveryRefreshIntervalMs = REFRESH_INTERVAL;
			policy.seedCandidateProvider = provider;
			return policy;
		}

		private static string AssertJoins(string name, AerospikeClient client)
		{
			string seeds = messages.Snapshot().FirstOrDefault(m => m.StartsWith(RESOLVED, StringComparison.Ordinal));
			Assert.IsNotNull(seeds, "no seed list logged: " + ConformanceEnv.Fmt(messages.Snapshot()));

			Await(() => client.Nodes.Length == 2 && ConformanceEnv.OwnedPartitions(client, ns) == Node.PARTITIONS ? "ok" : null);
			List<string> names = ConformanceEnv.NodeNames(client);
			int owned = ConformanceEnv.OwnedPartitions(client, ns);

			Console.WriteLine("DNS-DISCOVERY-RESULT " + name + " seeds='" + seeds + "' nodes=" + names.Count + ' ' +
				ConformanceEnv.Fmt(names) + " ownedPartitions=" + owned + '/' + Node.PARTITIONS);
			CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
			Assert.AreEqual(Node.PARTITIONS, owned);
			AssertKeysWork(name, client);
			return seeds;
		}

		private static void AssertKeysWork(string name, AerospikeClient client)
		{
			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(ns, "dns-discovery", i);
				client.Put(null, key, new Bin("v", i));
				Record record = client.Get(null, key);
				Assert.AreEqual(i, record.GetInt("v"));
			}
			Console.WriteLine("DNS-DISCOVERY-RESULT " + name + " keys=" + KEY_COUNT + " written and read");
		}

		/// <summary>
		/// The line the provider logs for the A records of both nodes: the names resolved and the number
		/// of hosts.
		/// </summary>
		private static string ASeedLine()
		{
			return RESOLVED + SEEDS_NAME + " resolved to 2 hosts";
		}

		/// <summary>
		/// The hosts the A records of both nodes resolve to, sorted as strings.
		/// </summary>
		private static List<string> ASeeds()
		{
			List<string> list = new List<string> {new Host(hostA, port).ToString(), new Host(hostB, port).ToString()};
			list.Sort(StringComparer.Ordinal);
			return list;
		}

		private static List<string> SeedStrings(AerospikeClient client)
		{
			List<string> list = new List<string>();

			foreach (Host seed in ClusterInternals.Seeds(client))
			{
				list.Add(seed.ToString());
			}
			list.Sort(StringComparer.Ordinal);
			return list;
		}

		private static void Control(params string[] verb)
		{
			string output = ConformanceEnv.Control(verb);
			Console.WriteLine("DNS-DISCOVERY-CONTROL " + ConformanceEnv.Fmt(verb) + ": " + output);
		}

		private static T Await<T>(Func<T> condition) where T : class
		{
			long deadline = Environment.TickCount64 + WAIT_MS;
			T value;

			while ((value = condition()) == null)
			{
				if (Environment.TickCount64 > deadline)
				{
					Assert.Fail("condition not met within " + WAIT_MS + "ms: " + ConformanceEnv.Fmt(messages.Snapshot()));
				}
				Thread.Sleep(50);
			}
			return value;
		}
	}
}
