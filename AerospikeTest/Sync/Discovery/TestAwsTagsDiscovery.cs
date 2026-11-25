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
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Aerospike.Client;
using MessageList = Aerospike.Test.ConformanceEnv.MessageList;

namespace Aerospike.Test
{
	/// <summary>
	/// <see cref="AwsEc2TagSeedCandidateProvider"/> against the two node cluster and stub EC2 Query API
	/// endpoint of the aerospike-client-conformance aws-tags environment. The client gets no static
	/// seed: every seed comes from the stub. Credentials come from the environment variables the values
	/// file sets to fixed fake values, so the environment source of the credential chain runs end to
	/// end. The stub returns each node as a running instance whose private IP is the node's published
	/// address, reachable from this host: node A on 127.0.0.1 and node B on another host address, both
	/// on the same port. Each node advertises a private address as its alternate access address, which
	/// <see cref="ClientPolicy.ipMap"/> maps to its published address. The seed list is read from the
	/// provider's debug log line. aws-tags runs only in execution mode thread; every case states its
	/// mode. The environment arrives as the environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>EC2_ENDPOINT</c>: the stub EC2 endpoint</item>
	/// <item><c>EC2_STUB_CONTROL</c>: the stub's control API, which sets its mode and delay and returns
	/// the requests it received</item>
	/// <item><c>AWS_REGION</c>: region the provider signs for</item>
	/// <item><c>ENV_NAMESPACE</c>: namespace, replication factor 1</item>
	/// <item><c>NODE_A_PORT</c>: port node A and node B are reachable on</item>
	/// <item><c>NODE_A_NAME</c>, <c>NODE_A_HOST</c>, <c>NODE_A_ADVERTISED_HOST</c>, <c>NODE_B_NAME</c>,
	/// <c>NODE_B_HOST</c>, <c>NODE_B_ADVERTISED_HOST</c>: node names, reachable addresses and advertised
	/// private addresses</item>
	/// <item><c>CLIENT_CONFIG</c>: YAML config file selecting provider aws-tags with the stub endpoint,
	/// passed to the client as the file URI in <c>AEROSPIKE_CLIENT_CONFIG_URL</c></item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestAwsTagsDiscovery
	{
		private const string TAG_KEY = "aerospike-cluster";
		private const string TAG_VALUE = "prod-a";
		private const string RETURNED = "AWS EC2 seed provider ";
		private const string FAILED = "Seed candidate provider aws-tags failed: ";
		private const string ERROR_REQUEST_ID = "ea966190-f9aa-478e-9ede-example";
		private const int REFRESH_INTERVAL = 1000;
		private const int TIMEOUT = 50;
		private const int COLD_CALL_MS = 700;
		private const int TRIP_OVERRUNS = 3;
		private const int WAIT_MS = 30000;
		private const int KEY_COUNT = 100;

		private static readonly Regex SEEDS = new Regex(
			"^AWS EC2 seed provider \\S+ returned (\\[.*\\]) from (\\d+) instances in (\\d+) pages$",
			RegexOptions.Compiled);

		private static readonly MessageList messages = new MessageList();
		private static readonly HttpClient http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };

		private static string stubControl;
		private static string endpoint;
		private static string region;
		private static string ns;
		private static int port;
		private static string nodeA;
		private static string hostA;
		private static string privateA;
		private static string nodeB;
		private static string hostB;
		private static string privateB;
		private static string config;
		private static string secret;
		private static string sessionToken;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("EC2_ENDPOINT", "EC2_STUB_CONTROL", "AWS_REGION", "ENV_NAMESPACE", "NODE_A_PORT",
				"NODE_A_NAME", "NODE_A_HOST", "NODE_A_ADVERTISED_HOST", "NODE_B_NAME", "NODE_B_HOST",
				"NODE_B_ADVERTISED_HOST", "CLIENT_CONFIG", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN");
			ConformanceEnv.RequireNoConfig();

			endpoint = ConformanceEnv.Get("EC2_ENDPOINT");
			stubControl = ConformanceEnv.Get("EC2_STUB_CONTROL");
			region = ConformanceEnv.Get("AWS_REGION");
			ns = ConformanceEnv.Get("ENV_NAMESPACE");
			port = ConformanceEnv.GetInt("NODE_A_PORT");
			nodeA = ConformanceEnv.Get("NODE_A_NAME");
			hostA = ConformanceEnv.Get("NODE_A_HOST");
			privateA = ConformanceEnv.Get("NODE_A_ADVERTISED_HOST");
			nodeB = ConformanceEnv.Get("NODE_B_NAME");
			hostB = ConformanceEnv.Get("NODE_B_HOST");
			privateB = ConformanceEnv.Get("NODE_B_ADVERTISED_HOST");
			config = ConformanceEnv.Get("CLIENT_CONFIG");
			secret = ConformanceEnv.Get("AWS_SECRET_ACCESS_KEY");
			sessionToken = ConformanceEnv.Get("AWS_SESSION_TOKEN");

			Log.SetLevel(Log.Level.DEBUG);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN || level == Log.Level.ERROR ||
					message.StartsWith(RETURNED, StringComparison.Ordinal))
				{
					messages.Add(message);
					Console.WriteLine("CLIENT-LOG " + level + ' ' + message.Replace('\n', ' '));
				}
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			Log.SetCallback(null);
		}

		[TestInitialize]
		public void Reset()
		{
			messages.Clear();
			Stub("reset", "");
		}

		[TestCleanup]
		public void ClearConfig()
		{
			Environment.SetEnvironmentVariable(ConformanceEnv.ConfigUrlVariable, null);
			Stub("settings", "{\"mode\": \"answer\"}");
		}

		[TestMethod]
		public void Case1BothNodesJoinThroughProvider()
		{
			using (AerospikeClient client = new AerospikeClient(NewPolicy(Provider(80)), Array.Empty<Host>()))
			{
				string seeds = AssertJoins("join", client);
				Assert.AreEqual(SeedList(hostA, hostB), seeds);

				List<StubRequest> requests = Requests();
				StubRequest request = requests[0];
				Console.WriteLine("AWS-TAGS-RESULT join requests=" + requests.Count + " params=" +
					ConformanceEnv.FmtMap(new SortedDictionary<string, string>(request.Params, StringComparer.Ordinal)) +
					" authorization=" + Regex.Replace(request.Authorization, "Signature=[0-9a-f]+", "Signature=<64 hex>") +
					" wellFormed=" + request.AuthorizationValid);

				SortedDictionary<string, string> expected = new SortedDictionary<string, string>(StringComparer.Ordinal)
				{
					{"Action", "DescribeInstances"},
					{"Version", "2016-11-15"},
					{"Filter.1.Name", "tag:" + TAG_KEY},
					{"Filter.1.Value.1", TAG_VALUE},
					{"Filter.2.Name", "instance-state-name"},
					{"Filter.2.Value.1", "running"}
				};

				foreach (StubRequest r in requests)
				{
					Assert.AreEqual(ConformanceEnv.FmtMap(expected),
						ConformanceEnv.FmtMap(new SortedDictionary<string, string>(r.Params, StringComparer.Ordinal)));
					Assert.IsTrue(r.AuthorizationValid, r.Authorization);
				}
			}
		}

		[TestMethod]
		public void Case4UnauthorizedOperationKeepsClientRunning()
		{
			using (AerospikeClient client = new AerospikeClient(NewPolicy(Provider(80)), Array.Empty<Host>()))
			{
				AssertJoins("unauthorized-before", client);
				messages.Clear();
				int sent = Requests().Count;
				Stub("settings", "{\"mode\": \"unauthorized\"}");

				string failure = Await(() => messages.Snapshot()
					.FirstOrDefault(m => m.StartsWith(FAILED, StringComparison.Ordinal)));
				string signature = Requests()[sent].Signature;
				Console.WriteLine("AWS-TAGS-WARN " + failure.Trim());

				Assert.IsTrue(failure.Contains("DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not " +
					"authorized to perform this operation. (request id " + ERROR_REQUEST_ID + ")"), failure);
				AssertNoSecret(failure, signature);

				List<string> names = ConformanceEnv.NodeNames(client);
				Console.WriteLine("AWS-TAGS-RESULT unauthorized nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names) +
					" connected=" + client.Connected + " secretInMessage=false signatureInMessage=false");
				CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
				AssertKeysWork("unauthorized", client);
			}
		}

		[TestMethod]
		public void Case5UnansweredStubTimesOutWithoutTrip()
		{
			// The provider's own timeout bounds each refresh in thread mode; the client enforces no
			// separate overrun on a refresh that returns within the refresh timeout.
			using (AerospikeClient client = new AerospikeClient(NewPolicy(Provider(TIMEOUT)), Array.Empty<Host>()))
			{
				AssertHangTimesOut("hang", client);
			}
		}

		[TestMethod]
		public void Case6YamlUnansweredStubTimesOutWithoutTrip()
		{
			ConformanceEnv.SetConfig(config);

			using (AerospikeClient client = new AerospikeClient(new ClientPolicy(), Array.Empty<Host>()))
			{
				Assert.IsNotNull(ConfigTestHelpers.GetConfigProvider(client));
				ClientPolicy merged = ConfigTestHelpers.GetClientPolicy(client);
				AwsEc2TagSeedCandidateProvider provider = (AwsEc2TagSeedCandidateProvider)merged.seedCandidateProvider;
				Console.WriteLine("AWS-TAGS-RESULT yaml provider region=" + provider.Region + " filters=" +
					FmtFilters(provider.Filters) + " port=" + provider.Port + " timeout=" + provider.Timeout +
					" endpoint=" + provider.Endpoint + " executionMode=" + merged.discoveryExecutionMode +
					" refreshTimeout=" + merged.discoveryRefreshTimeoutMs + " ipMap=" + ConformanceEnv.FmtMap(merged.ipMap ?? new Dictionary<string, string>()));
				Assert.AreEqual(endpoint, provider.Endpoint);
				Assert.AreEqual(TIMEOUT, provider.Timeout);
				Assert.AreEqual(DiscoveryExecutionMode.THREAD, merged.discoveryExecutionMode);
				Assert.AreEqual(TIMEOUT, merged.discoveryRefreshTimeoutMs);
				AssertHangTimesOut("yaml-hang", client);
			}
		}

		[TestMethod]
		public void Case7ColdCallsWorkWithDefaultTimeouts()
		{
			Stub("settings", "{\"delay_ms\": " + COLD_CALL_MS + "}");
			AwsEc2TagSeedCandidateProvider provider = AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(region)
				.TagFilter(TAG_KEY, TAG_VALUE)
				.Port(port)
				.Endpoint(endpoint)
				.Build();
			ClientPolicy policy = NewPolicy(provider);
			long begin = Environment.TickCount64;

			using (AerospikeClient client = new AerospikeClient(policy, Array.Empty<Host>()))
			{
				long constructed = Environment.TickCount64 - begin;
				AssertJoins("cold", client);
				Await(() => ReturnedCount() >= 3 ? "ok" : null);

				long returned = ReturnedCount();
				List<string> refreshWarnings = messages.Snapshot()
					.Where(m => m.StartsWith("Seed candidate ", StringComparison.Ordinal)).ToList();
				Console.WriteLine("AWS-TAGS-RESULT cold delayMs=" + COLD_CALL_MS + " constructedMs=" + constructed +
					" timeout=" + provider.Timeout + " refreshTimeout=" + policy.discoveryRefreshTimeoutMs +
					" refreshesReturned=" + returned + " refreshWarnings=" + ConformanceEnv.Fmt(refreshWarnings));
				Assert.AreEqual(AwsEc2TagSeedCandidateProvider.DefaultTimeout, provider.Timeout);
				Assert.AreEqual(new ClientPolicy().discoveryRefreshTimeoutMs, policy.discoveryRefreshTimeoutMs);
				Assert.IsTrue(constructed >= COLD_CALL_MS, constructed + "ms");
				Assert.AreEqual(0, refreshWarnings.Count, ConformanceEnv.Fmt(refreshWarnings));
				AssertKeysWork("cold", client);
			}
		}

		[TestMethod]
		public void Case8TendModeFailsConstruction()
		{
			ClientPolicy policy = NewPolicy(Provider(80));
			policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => new AerospikeClient(policy, Array.Empty<Host>()).Close());
			int requests = Requests().Count;
			Console.WriteLine("AWS-TAGS-RESULT tend " + ae.Message + " requests=" + requests);
			Assert.IsTrue(ae.Message.Contains("must support a deadline") && ae.Message.Contains("TEND"), ae.Message);
			Assert.AreEqual(0, requests);
		}

		private static void AssertHangTimesOut(string name, AerospikeClient client)
		{
			AssertJoins(name + "-before", client);
			messages.Clear();
			Stub("settings", "{\"mode\": \"hang\"}");

			Await(() => messages.Snapshot().Any(m => m.StartsWith(FAILED, StringComparison.Ordinal)) ? "ok" : null);
			Host[] lastGood = ClusterInternals.Seeds(client);

			Await(() => messages.Snapshot().Count(m => m.StartsWith(FAILED, StringComparison.Ordinal)) > TRIP_OVERRUNS ? "ok" : null);
			int failures = 0;

			foreach (string m in messages.Snapshot())
			{
				Console.WriteLine("AWS-TAGS-WARN " + m.Trim());

				if (m.StartsWith(FAILED, StringComparison.Ordinal))
				{
					failures++;
					Assert.IsTrue(m.Contains("AWS EC2 seed provider: DescribeInstances timed out after " + TIMEOUT + "ms"), m);
				}
			}

			Host[] seeds = ClusterInternals.Seeds(client);
			List<string> names = ConformanceEnv.NodeNames(client);
			Console.WriteLine("AWS-TAGS-RESULT " + name + " refreshFailures=" + failures + " seeds=" +
				ConformanceEnv.Fmt(seeds) + " lastGoodKept=" + ReferenceEquals(seeds, lastGood) + " nodes=" + names.Count +
				' ' + ConformanceEnv.Fmt(names) + " connected=" + client.Connected);
			Assert.IsFalse(messages.Snapshot().Any(m => m.StartsWith("Seed candidate provider exceeded tend deadline",
				StringComparison.Ordinal)));
			Assert.IsTrue(failures > TRIP_OVERRUNS, "refresh failures: " + failures);
			Assert.AreSame(lastGood, seeds);
			Assert.AreEqual(SeedList(hostA, hostB), ConformanceEnv.Fmt(seeds));
			CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
			AssertKeysWork(name, client);
		}

		private static AwsEc2TagSeedCandidateProvider Provider(int timeout)
		{
			return AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(region)
				.TagFilter(TAG_KEY, TAG_VALUE)
				.Port(port)
				.Timeout(timeout)
				.Endpoint(endpoint)
				.Build();
		}

		private static ClientPolicy NewPolicy(AwsEc2TagSeedCandidateProvider provider)
		{
			ClientPolicy policy = new ClientPolicy();
			policy.useServicesAlternate = true;
			policy.ipMap = new Dictionary<string, string>();
			policy.ipMap[privateA] = hostA;
			policy.ipMap[privateB] = hostB;
			policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
			policy.discoveryRefreshIntervalMs = REFRESH_INTERVAL;
			policy.seedCandidateProvider = provider;
			return policy;
		}

		private static string AssertJoins(string name, AerospikeClient client)
		{
			string seeds = messages.Snapshot().Select(Seeds).FirstOrDefault(s => s != null);
			Assert.IsNotNull(seeds, "no seed list logged: " + ConformanceEnv.Fmt(messages.Snapshot()));

			Await(() => client.Nodes.Length == 2 && ConformanceEnv.OwnedPartitions(client, ns) == Node.PARTITIONS ? "ok" : null);
			List<string> names = ConformanceEnv.NodeNames(client);
			int owned = ConformanceEnv.OwnedPartitions(client, ns);

			Console.WriteLine("AWS-TAGS-RESULT " + name + " seeds=" + seeds + " nodes=" + names.Count + ' ' +
				ConformanceEnv.Fmt(names) + " ownedPartitions=" + owned + '/' + Node.PARTITIONS);
			CollectionAssert.AreEqual(new List<string> {nodeA, nodeB}, names);
			Assert.AreEqual(Node.PARTITIONS, owned);
			AssertKeysWork(name, client);
			return seeds;
		}

		private static string Seeds(string message)
		{
			Match m = SEEDS.Match(message);
			return m.Success ? m.Groups[1].Value : null;
		}

		private static int ReturnedCount()
		{
			return messages.Snapshot().Count(m => Seeds(m) != null);
		}

		private static string SeedList(params string[] hosts)
		{
			List<string> list = new List<string>(hosts);
			list.Sort(StringComparer.Ordinal);

			StringBuilder sb = new StringBuilder("[");

			foreach (string host in list)
			{
				sb.Append(sb.Length > 1 ? ", " : "").Append(host).Append(' ').Append(port);
			}
			return sb.Append(']').ToString();
		}

		private static string FmtFilters(IReadOnlyDictionary<string, IReadOnlyList<string>> filters)
		{
			List<string> parts = new List<string>();

			foreach (KeyValuePair<string, IReadOnlyList<string>> entry in filters)
			{
				parts.Add(entry.Key + "=" + ConformanceEnv.Fmt(entry.Value));
			}
			return ConformanceEnv.Fmt(parts);
		}

		private static void AssertNoSecret(string text, string signature)
		{
			Assert.IsFalse(text.Contains(secret), text);
			Assert.IsFalse(text.Contains(sessionToken), text);
			Assert.IsFalse(text.Contains(signature), text);
		}

		private static void AssertKeysWork(string name, AerospikeClient client)
		{
			for (int i = 0; i < KEY_COUNT; i++)
			{
				Key key = new Key(ns, "aws-tags-discovery", i);
				client.Put(null, key, new Bin("v", i));
				Record record = client.Get(null, key);
				Assert.AreEqual(i, record.GetInt("v"));
			}
			Console.WriteLine("AWS-TAGS-RESULT " + name + " keys=" + KEY_COUNT + " written and read");
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

		private static List<StubRequest> Requests()
		{
			List<StubRequest> requests = new List<StubRequest>();

			using (JsonDocument document = JsonDocument.Parse(Stub("requests", null)))
			{
				foreach (JsonElement element in document.RootElement.EnumerateArray())
				{
					requests.Add(new StubRequest(element));
				}
			}
			return requests;
		}

		private static string Stub(string path, string body)
		{
			using (HttpRequestMessage request = new HttpRequestMessage(
				body == null ? HttpMethod.Get : HttpMethod.Post, stubControl + '/' + path))
			{
				if (body != null)
				{
					request.Content = new StringContent(body, Encoding.UTF8, "application/json");
				}

				using (HttpResponseMessage response = http.Send(request))
				{
					string text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

					if ((int)response.StatusCode != 200)
					{
						throw new InvalidOperationException("Stub EC2 control " + path + " failed: HTTP " +
							(int)response.StatusCode + ' ' + text);
					}
					return text;
				}
			}
		}

		private sealed class StubRequest
		{
			internal readonly Dictionary<string, string> Params = new Dictionary<string, string>();
			internal readonly string Authorization;
			internal readonly bool AuthorizationValid;
			internal readonly string Signature;

			internal StubRequest(JsonElement request)
			{
				foreach (JsonProperty property in request.GetProperty("params").EnumerateObject())
				{
					Params[property.Name] = property.Value.ValueKind == JsonValueKind.String ?
						property.Value.GetString() : property.Value.GetRawText();
				}
				Authorization = request.GetProperty("authorization").GetString();
				AuthorizationValid = request.GetProperty("authorization_valid").GetBoolean();
				Signature = request.GetProperty("signature").GetString();
			}
		}
	}
}
