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
using Aerospike.Client.Config;

namespace Aerospike.Test
{
	[TestClass]
	public class TestConfigLoadYaml : TestSync
	{
		[TestMethod]
		public void ConfigLoadGoodYaml()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.GoodConfigYaml);
			AerospikeClient configClient = scope.Client;

			IConfigProvider provider = ConfigTestHelpers.GetConfigProvider(configClient);
			Assert.IsNotNull(provider);
			Assert.IsNotNull(provider.ConfigurationData);

			ClientPolicy clientPolicy = ConfigTestHelpers.GetClientPolicy(configClient);
			Assert.AreEqual("config_test_app", clientPolicy.AppId);
			Assert.AreEqual(42, clientPolicy.maxConnsPerNode);
			Assert.AreEqual(3, clientPolicy.minConnsPerNode);

			Policy readPolicy = ConfigTestHelpers.GetMergedReadPolicy(configClient);
			Assert.AreEqual(Replica.PREFER_RACK, readPolicy.replica);
			Assert.AreEqual(20, readPolicy.maxRetries);
			Assert.AreEqual(750, readPolicy.socketTimeout);

			BatchPolicy batchPolicy = ConfigTestHelpers.GetMergedBatchPolicy(configClient);
			Assert.AreEqual(10, batchPolicy.maxConcurrentThreads);
			Assert.IsTrue(batchPolicy.allowInline);

			WritePolicy writePolicy = ConfigTestHelpers.GetMergedWritePolicy(configClient);
			Assert.IsTrue(writePolicy.durableDelete);

			Assert.IsFalse(configClient.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void ConfigLoadInvalidYaml()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.InvalidYaml);
			Assert.IsNull(ConfigTestHelpers.GetConfigProvider(scope.Client));
		}

		[TestMethod]
		public void ConfigLoadVersion10Rejects11Field()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.Version10With11FieldYaml);
			Assert.IsNull(ConfigTestHelpers.GetConfigProvider(scope.Client));
		}

		[TestMethod]
		public void ConfigLoadVersion11AcceptsErrorDetailVerbosity()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.Version11WithErrorDetailYaml);
			IConfigProvider provider = ConfigTestHelpers.GetConfigProvider(scope.Client);

			Assert.IsNotNull(provider);
			Assert.AreEqual(2, provider.ConfigurationData.dynamicConfig.read.error_detail_verbosity);
			Assert.AreEqual(3, provider.ConfigurationData.dynamicConfig.write.error_detail_verbosity);
			Assert.AreEqual(11, ConfigTestHelpers.GetClientPolicy(scope.Client).maxConnsPerNode);
		}

		[TestMethod]
		public void ConfigReloadUpdatesDynamicPolicy()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.ReloadInitialYaml);
			AerospikeClient configClient = scope.Client;

			Assert.AreEqual(10, ConfigTestHelpers.GetMergedReadPolicy(configClient).maxRetries);
			Assert.AreEqual("reload_test", ConfigTestHelpers.GetClientPolicy(configClient).AppId);

			scope.RewriteYaml(ConfigTestHelpers.ReloadUpdatedYaml);
			ConfigTestHelpers.WaitForReadMaxRetries(configClient, 33);

			Assert.AreEqual("reload_test_updated", ConfigTestHelpers.GetClientPolicy(configClient).AppId);
		}

		[TestMethod]
		public void ConfigMissingFile()
		{
			string previousConfigUrl = Environment.GetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL");
			string missingPath = Path.Combine(Path.GetTempPath(), $"as-missing-{Guid.NewGuid():N}.yaml");
			Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", new Uri(missingPath).AbsoluteUri);

			try
			{
				AerospikeClient configClient = new(ConfigClientScope.CreateClientPolicy(), SuiteHelpers.hosts);
				try
				{
					Assert.IsNull(ConfigTestHelpers.GetConfigProvider(configClient));
				}
				finally
				{
					configClient.Close();
				}
			}
			finally
			{
				Environment.SetEnvironmentVariable("AEROSPIKE_CLIENT_CONFIG_URL", previousConfigUrl);
			}
		}
	}
}
