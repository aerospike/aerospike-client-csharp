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
using System.Collections.Generic;
using Aerospike.Client;
using Aerospike.Client.Config;

namespace Aerospike.Test
{
	[TestClass]
	public class TestDiscoveryYaml
	{
		private string previousConfigUrl;

		[TestInitialize]
		public void SetUp()
		{
			previousConfigUrl = Environment.GetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL");
		}

		[TestCleanup]
		public void TearDown()
		{
			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", previousConfigUrl);
		}

		private static ClientPolicy OfflinePolicy()
		{
			ClientPolicy policy = ConfigClientScope.CreateClientPolicy();
			policy.failIfNotConnected = false;
			policy.timeout = 100;
			return policy;
		}

		private static (AerospikeClient client, string path) Open(string yaml, ClientPolicy code = null)
		{
			string path = Path.Combine(Path.GetTempPath(), $"as-discovery-{Guid.NewGuid():N}.yaml");
			File.WriteAllText(path, yaml);
			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", new Uri(path).AbsoluteUri);
			AerospikeClient client = new(code ?? OfflinePolicy(), new Host("127.0.0.1", 1));
			return (client, path);
		}

		private static void Close(AerospikeClient client, string path)
		{
			client?.Close();

			if (path != null && File.Exists(path))
			{
				File.Delete(path);
			}
		}

		[TestMethod]
		public void PortMapBuildsPortMapTranslator()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    translation:
				      provider: port-map
				      port_map:
				        "10.0.0.1:3000": "lb.example.com:30001"
				        "[2001:db8::1]:3000": "lb.example.com:30002"
				        BB9020011AC4202: "lb.example.com:30003"
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsInstanceOfType(policy.addressTranslator, typeof(PortMapAddressTranslator));

				Host mapped = policy.addressTranslator.Translate(
					new Endpoint("X", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD));
				Assert.AreEqual("lb.example.com", mapped.name);
				Assert.AreEqual(30001, mapped.port);

				mapped = policy.addressTranslator.Translate(
					new Endpoint("BB9020011AC4202", null, "10.0.0.9", 3000, Endpoint.SourceList.STANDARD));
				Assert.AreEqual(30003, mapped.port);
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void StaticMapAndPassThroughAndMergePolicy()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    discovery:
				      provider: static
				      merge_policy: replace
				      refresh_interval_ms: 60000
				      tend_refresh_deadline_ms: 200
				      execution_mode: tend
				      refresh_timeout_ms: 5000
				    translation:
				      provider: pass-through
				      on_miss: strict
				    use_services_alternate: true
				    ip_map:
				      10.0.0.1: 192.168.1.1
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsNull(policy.seedCandidateProvider);
				Assert.AreEqual(SeedMergePolicy.REPLACE, policy.seedMergePolicy);
				Assert.AreEqual(60000, policy.discoveryRefreshIntervalMs);
				Assert.AreEqual(200, policy.discoveryTendRefreshDeadlineMs);
				Assert.AreEqual(DiscoveryExecutionMode.TEND, policy.discoveryExecutionMode);
				Assert.AreEqual(5000, policy.discoveryRefreshTimeoutMs);
				Assert.AreSame(PassThroughAddressTranslator.Instance, policy.addressTranslator);
				Assert.AreEqual(TranslationMissMode.STRICT, policy.translationOnMiss);
				Assert.IsTrue(policy.useServicesAlternate);
				Assert.AreEqual("192.168.1.1", policy.ipMap["10.0.0.1"]);
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void StaticMapBuildsTranslator()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    translation:
				      provider: static-map
				      static_map:
				        10.0.0.1: 192.168.1.1
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsInstanceOfType(policy.addressTranslator, typeof(StaticMapAddressTranslator));
				Host mapped = policy.addressTranslator.Translate(
					new Endpoint("BB9", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD));
				Assert.AreEqual("192.168.1.1", mapped.name);
				Assert.AreEqual(3000, mapped.port);
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void DnsProviderBuildsDnsSeedProvider()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    discovery:
				      provider: dns
				      refresh_interval_ms: 30000
				      tend_refresh_deadline_ms: 100
				      dns:
				        names:
				          - seeds.example.com
				        port: 3100
				        timeout_ms: 50
				        servers:
				          - 127.0.0.1
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsInstanceOfType(policy.seedCandidateProvider, typeof(DnsSeedCandidateProvider));
				DnsSeedCandidateProvider dns = (DnsSeedCandidateProvider)policy.seedCandidateProvider;
				Assert.AreEqual(3100, dns.Port);
				Assert.AreEqual(50, dns.Timeout);
				Assert.AreEqual(1, dns.Names.Count);
				Assert.AreEqual("seeds.example.com", dns.Names[0]);
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void AwsTagsWithoutAwsBlockRejected()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    discovery:
				      provider: aws-tags
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				Assert.IsNull(ConfigTestHelpers.GetConfigProvider(client));
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void AwsTagsProviderBuildsAwsSeedProvider()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    discovery:
				      provider: aws-tags
				      merge_policy: merge
				      refresh_interval_ms: 30000
				      tend_refresh_deadline_ms: 100
				      refresh_timeout_ms: 1000
				      aws:
				        region: us-east-1
				        filters:
				          - name: tag:aerospike-cluster
				            values:
				              - prod-a
				        address_field: private_ip
				        port: 3100
				        timeout_ms: 80
				        init_timeout_ms: 100
				        endpoint: http://127.0.0.1:59999
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsInstanceOfType(policy.seedCandidateProvider, typeof(AwsEc2TagSeedCandidateProvider));
				AwsEc2TagSeedCandidateProvider aws = (AwsEc2TagSeedCandidateProvider)policy.seedCandidateProvider;
				Assert.AreEqual("us-east-1", aws.Region);
				Assert.AreEqual(3100, aws.Port);
				Assert.AreEqual(80, aws.Timeout);
				Assert.AreEqual(100, aws.InitTimeout);
				Assert.AreEqual(AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP, aws.Address);
				Assert.AreEqual("http://127.0.0.1:59999", aws.Endpoint);
				Assert.AreEqual(1, aws.Filters.Count);
				Assert.IsTrue(aws.Filters.ContainsKey("tag:aerospike-cluster"));
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void InvalidPortMapRejected()
		{
			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    translation:
				      provider: port-map
				      port_map:
				        "10.0.0.1:3000": "lb.example.com"
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				Assert.IsNull(ConfigTestHelpers.GetConfigProvider(client));
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void Version10RejectsDiscoveryBlock()
		{
			const string yaml = """
				version: 1.0.0
				static:
				  client:
				    discovery:
				      merge_policy: replace
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml);

			try
			{
				Assert.IsNull(ConfigTestHelpers.GetConfigProvider(client));
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void RegisteredProviderAndTranslator()
		{
			Dictionary<string, object> seenProviderOptions = null;
			Dictionary<string, object> seenTranslatorOptions = null;

			ClientPolicy code = OfflinePolicy();
			code.RegisterSeedCandidateProvider("recording", options =>
			{
				seenProviderOptions = new Dictionary<string, object>(options);
				return new StaticSeedCandidateProvider(new Host("10.0.0.5", 3000));
			});
			code.RegisterAddressTranslator("table", options =>
			{
				seenTranslatorOptions = new Dictionary<string, object>(options);
				return PassThroughAddressTranslator.Instance;
			});

			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    discovery:
				      provider: recording
				      region: us-west
				      refresh_interval_ms: 45000
				    translation:
				      provider: table
				      table_id: 7
				      on_miss: strict
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml, code);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.IsInstanceOfType(policy.seedCandidateProvider, typeof(StaticSeedCandidateProvider));
				Assert.AreSame(PassThroughAddressTranslator.Instance, policy.addressTranslator);
				Assert.AreEqual(45000, policy.discoveryRefreshIntervalMs);
				Assert.AreEqual(TranslationMissMode.STRICT, policy.translationOnMiss);
				Assert.IsNotNull(seenProviderOptions);
				Assert.AreEqual("us-west", seenProviderOptions["region"]);
				Assert.IsNotNull(seenTranslatorOptions);
				Assert.AreEqual("7", seenTranslatorOptions["table_id"].ToString());
			}
			finally
			{
				Close(client, path);
			}
		}

		[TestMethod]
		public void RegisterRejectsBuiltInNames()
		{
			ClientPolicy policy = new ClientPolicy();
			Assert.Throws<AerospikeException>(() =>
				policy.RegisterSeedCandidateProvider("dns", _ => new StaticSeedCandidateProvider()));
			Assert.Throws<AerospikeException>(() =>
				policy.RegisterAddressTranslator("port-map", _ => PassThroughAddressTranslator.Instance));
		}

		[TestMethod]
		public void OnMissOnlyKeepsCodeTranslator()
		{
			ClientPolicy code = OfflinePolicy();
			code.addressTranslator = PassThroughAddressTranslator.Instance;

			const string yaml = """
				version: 1.2.0
				static:
				  client:
				    translation:
				      on_miss: strict
				dynamic:
				  client:
				    tend_interval: 1000
				""";

			(AerospikeClient client, string path) = Open(yaml, code);

			try
			{
				ClientPolicy policy = ConfigTestHelpers.GetClientPolicy(client);
				Assert.AreSame(PassThroughAddressTranslator.Instance, policy.addressTranslator);
				Assert.AreEqual(TranslationMissMode.STRICT, policy.translationOnMiss);
			}
			finally
			{
				Close(client, path);
			}
		}
	}
}
