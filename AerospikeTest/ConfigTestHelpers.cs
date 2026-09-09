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
using System.Reflection;
using Aerospike.Client;
using Aerospike.Client.Config;
using Microsoft.Extensions.Configuration;

namespace Aerospike.Test
{
	/// <summary>
	/// Creates dedicated clients that load YAML from a temp file via AEROSPIKE_CLIENT_CONFIG_URL.
	/// </summary>
	internal sealed class ConfigClientScope : IDisposable
	{
		private readonly string yamlPath;
		private readonly string previousConfigUrl;

		public AerospikeClient Client { get; }

		public string YamlPath => yamlPath;

		private ConfigClientScope(AerospikeClient client, string yamlPath, string previousConfigUrl)
		{
			Client = client;
			this.yamlPath = yamlPath;
			this.previousConfigUrl = previousConfigUrl;
		}

		public static ConfigClientScope Create(string yamlContent)
		{
			string previousConfigUrl = Environment.GetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL");
			string yamlPath = Path.Combine(Path.GetTempPath(), $"as-config-{Guid.NewGuid():N}.yaml");
			File.WriteAllText(yamlPath, yamlContent);
			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", new Uri(yamlPath).AbsoluteUri);

			ClientPolicy policy = CreateClientPolicy();
			AerospikeClient client = new(policy, SuiteHelpers.hosts);
			return new ConfigClientScope(client, yamlPath, previousConfigUrl);
		}

		public void RewriteYaml(string yamlContent)
		{
			File.WriteAllText(yamlPath, yamlContent);
			ConfigTestHelpers.TriggerConfigReload(Client);
		}

		public static ClientPolicy CreateClientPolicy()
		{
			ClientPolicy policy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate
			};

			if (SuiteHelpers.user != null && SuiteHelpers.user.Length > 0)
			{
				policy.user = SuiteHelpers.user;
				policy.password = SuiteHelpers.password;
			}

			return policy;
		}

		public void Dispose()
		{
			Client?.Close();

			if (yamlPath != null && File.Exists(yamlPath))
			{
				File.Delete(yamlPath);
			}

			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", previousConfigUrl);
		}
	}

	internal sealed class ConfigAsyncClientScope : IDisposable
	{
		private readonly string yamlPath;
		private readonly string previousConfigUrl;

		public AsyncClient Client { get; }

		private ConfigAsyncClientScope(AsyncClient client, string yamlPath, string previousConfigUrl)
		{
			Client = client;
			this.yamlPath = yamlPath;
			this.previousConfigUrl = previousConfigUrl;
		}

		public static ConfigAsyncClientScope Create(string yamlContent)
		{
			string previousConfigUrl = Environment.GetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL");
			string yamlPath = Path.Combine(Path.GetTempPath(), $"as-async-config-{Guid.NewGuid():N}.yaml");
			File.WriteAllText(yamlPath, yamlContent);
			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", new Uri(yamlPath).AbsoluteUri);

			AsyncClientPolicy policy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate
			};

			if (SuiteHelpers.user != null && SuiteHelpers.user.Length > 0)
			{
				policy.user = SuiteHelpers.user;
				policy.password = SuiteHelpers.password;
			}

			AsyncClient client = new(policy, SuiteHelpers.hosts);
			return new ConfigAsyncClientScope(client, yamlPath, previousConfigUrl);
		}

		public void Dispose()
		{
			Client?.Close();

			if (yamlPath != null && File.Exists(yamlPath))
			{
				File.Delete(yamlPath);
			}

			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", previousConfigUrl);
		}
	}

	internal static class ConfigTestHelpers
	{
		public static IConfigProvider GetConfigProvider(AerospikeClient client)
		{
			FieldInfo field = typeof(AerospikeClient).GetField("configProvider",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (IConfigProvider)field.GetValue(client);
		}

		public static ClientPolicy GetClientPolicy(AerospikeClient client)
		{
			FieldInfo field = typeof(AerospikeClient).GetField("clientPolicy",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (ClientPolicy)field.GetValue(client);
		}

		public static Policy GetMergedReadPolicy(AerospikeClient client)
		{
			FieldInfo field = typeof(AerospikeClient).GetField("mergedReadPolicyDefault",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (Policy)field.GetValue(client);
		}

		public static BatchPolicy GetMergedBatchPolicy(AerospikeClient client)
		{
			FieldInfo field = typeof(AerospikeClient).GetField("mergedBatchPolicyDefault",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (BatchPolicy)field.GetValue(client);
		}

		public static WritePolicy GetMergedWritePolicy(AerospikeClient client)
		{
			FieldInfo field = typeof(AerospikeClient).GetField("mergedWritePolicyDefault",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (WritePolicy)field.GetValue(client);
		}

		public static void WaitForReadMaxRetries(AerospikeClient client, int expectedMaxRetries, int timeoutMs = 15000)
		{
			int attempts = timeoutMs / 250;
			for (int i = 0; i < attempts; i++)
			{
				if (GetMergedReadPolicy(client).maxRetries == expectedMaxRetries)
				{
					return;
				}
				TriggerConfigReload(client);
				Util.Sleep(250);
			}

			int actual = GetMergedReadPolicy(client).maxRetries;
			Assert.Fail($"Expected read max_retries {expectedMaxRetries}, but was {actual} after {timeoutMs}ms");
		}

		/// <summary>
		/// Force the YAML provider and cluster to reload config from disk. File-watch based reload is
		/// unreliable on some platforms (especially Linux CI), so reload tests call this after rewriting
		/// the temp config file.
		/// </summary>
		public static void TriggerConfigReload(AerospikeClient client)
		{
			IConfigProvider provider = GetConfigProvider(client);
			if (provider is not YamlConfigProvider yamlProvider)
			{
				return;
			}

			Type providerType = typeof(YamlConfigProvider);
			FieldInfo configRootField = providerType.GetField("configRoot",
				BindingFlags.Instance | BindingFlags.NonPublic);
			FieldInfo modifiedField = providerType.GetField("modified",
				BindingFlags.Instance | BindingFlags.NonPublic);

			IConfigurationRoot configRoot = (IConfigurationRoot)configRootField.GetValue(yamlProvider);
			configRoot.Reload();
			modifiedField.SetValue(yamlProvider, true);

			FieldInfo clusterField = typeof(AerospikeClient).GetField("cluster",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			Cluster cluster = (Cluster)clusterField.GetValue(client);
			MethodInfo loadConfiguration = typeof(Cluster).GetMethod("LoadConfiguration",
				BindingFlags.Instance | BindingFlags.NonPublic);
			loadConfiguration.Invoke(cluster, null);
		}

		public const string GoodConfigYaml = """
			version: 1.0.0
			static:
			  client:
			    config_interval: 5000
			    max_connections_per_node: 42
			    min_connections_per_node: 3
			dynamic:
			  client:
			    app_id: config_test_app
			    max_error_rate: 22
			    tend_interval: 500
			  read:
			    replica: PREFER_RACK
			    max_retries: 20
			    socket_timeout: 750
			  batch_read:
			    max_concurrent_threads: 10
			    allow_inline: true
			  write:
			    durable_delete: true
			  metrics:
			    latency_shift: 2
			    latency_columns: 7
			    labels:
			      region: us-west
			""";

		public const string InvalidYaml = """
			not: valid: yaml: [[[
			""";

		public const string Version10With11FieldYaml = """
			version: 1.0.0
			static:
			  client:
			    max_connections_per_node: 10
			dynamic:
			  read:
			    error_detail_verbosity: 1
			""";

		public const string Version11WithErrorDetailYaml = """
			version: 1.1.0
			static:
			  client:
			    max_connections_per_node: 11
			dynamic:
			  read:
			    error_detail_verbosity: 2
			  write:
			    error_detail_verbosity: 3
			""";

		public const string MetricsDisabledYaml = """
			version: 1.0.0
			dynamic:
			  metrics:
			    enable: false
			""";

		public const string ReloadInitialYaml = """
			version: 1.0.0
			static:
			  client:
			    config_interval: 250
			dynamic:
			  client:
			    tend_interval: 250
			    app_id: reload_test
			  read:
			    max_retries: 10
			""";

		public const string ReloadUpdatedYaml = """
			version: 1.0.0
			static:
			  client:
			    config_interval: 250
			dynamic:
			  client:
			    tend_interval: 250
			    app_id: reload_test_updated
			  read:
			    max_retries: 33
			""";

		public const string ReloadRemoveDynamicYaml = """
			version: 1.0.0
			static:
			  client:
			    config_interval: 250
			""";

		public const string MetricsEnabledYaml = """
			version: 1.0.0
			dynamic:
			  metrics:
			    enable: true
			""";

		public const string AsyncStaticConfigYaml = """
			version: 1.0.0
			static:
			  client:
			    async_max_connections_per_node: 55
			dynamic:
			  client:
			    app_id: async_static_test
			""";

		public static AsyncClientPolicy GetAsyncClientPolicy(AsyncClient client)
		{
			FieldInfo field = typeof(AsyncClient).GetField("clientPolicy",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
			return (AsyncClientPolicy)field.GetValue(client);
		}

		public static int GetAsyncMaxConnsPerNode(AsyncClient client)
		{
			FieldInfo clusterField = typeof(AerospikeClient).GetField("cluster",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			AsyncCluster cluster = (AsyncCluster)clusterField.GetValue(client);
			FieldInfo maxConnsField = typeof(AsyncCluster).GetField("asyncMaxConnsPerNode",
				BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			return (int)maxConnsField.GetValue(cluster);
		}

		public static void WaitForConfigurationDataNull(AerospikeClient client, int timeoutMs = 15000)
		{
			int attempts = timeoutMs / 250;
			for (int i = 0; i < attempts; i++)
			{
				if (GetConfigProvider(client)?.ConfigurationData == null)
				{
					return;
				}
				TriggerConfigReload(client);
				Util.Sleep(250);
			}

			Assert.Fail("Expected configuration data to be cleared after dynamic section removal");
		}
	}
}
