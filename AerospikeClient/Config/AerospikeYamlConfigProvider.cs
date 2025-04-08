/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

using Microsoft.Extensions.Configuration;
using Aerospike.Client.Config;

namespace Aerospike.Client
{
    public class AerospikeYamlConfigProvider : IAerospikeConfigProvider
    {
		#region yaml property names
		const string READ_MODE_AP = "read_mode_ap";
		const string READ_MODE_SC = "read_mode_sc";
		const string CONNECT_TIMEOUT = "connect_timeout";
		const string FAIL_ON_FILTERED_OUT = "fail_on_filtered_out";
		const string REPLICA = "replica";
		const string SLEEP_BETWEEN_RETRIES = "sleep_between_retries";
		const string SOCKET_TIMEOUT = "socket_timeout";
		const string TIMEOUT_DLEAY = "timeout_delay";
		const string MAX_RETRIES = "max_retries";
		const string DURABLE_DELETE = "durable_delete";
		const string MAX_CONCURRENT_THREADS = "max_concurrent_threads";
		const string ALLOW_INLINE = "allow_inline";
		const string ALLOW_INLINE_SSD = "allow_inline_ssd";
		const string RESPOND_ALL_KEYS = "respond_all_keys";
		const string SEND_KEY = "send_key";
		const string TOTAL_TIMEOUT = "total_timeout";
        #endregion

        public int? Interval { get; private set; }

        private IConfigurationRoot configRoot;

		public ConfigurationData ConfigurationData { get; private set; }

        public string YamlFilePath;

		public AerospikeYamlConfigProvider()
        {
            YamlFilePath = "." + Path.DirectorySeparatorChar + "aerospikeconfig.yaml";
            configRoot = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
                .Build();
            ConfigurationData = new()
            {
                metaData = new MetaData(),
                staticProperties = new StaticProperties(),
                dynamicProperties = new DynamicProperties()
            };
            configRoot.GetSection("metadata").Bind(ConfigurationData.metaData);
            configRoot.GetSection("static").Bind(ConfigurationData.staticProperties);
            configRoot.GetSection("dynamic").Bind(ConfigurationData.dynamicProperties);
        }

		public AerospikeYamlConfigProvider(string path)
        {
            YamlFilePath = path;
            configRoot = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
				.Build();
            ConfigurationData = new()
            {
                metaData = new MetaData(),
                staticProperties = new StaticProperties(),
                dynamicProperties = new DynamicProperties()
            };
            configRoot.GetSection("metadata").Bind(ConfigurationData.metaData);
            configRoot.GetSection("static").Bind(ConfigurationData.staticProperties);
            configRoot.GetSection("dynamic").Bind(ConfigurationData.dynamicProperties);
        }

        public void Watch()
        {
            _ = this.configRoot.GetReloadToken().RegisterChangeCallback(_ => {
                ProcessDynamicConfig();
                Watch();
            }, null);
        }

        public void InitalizeConfig()
		{
            ProcessStaticConfig();
            ProcessDynamicConfig();
        }

		public void UpdateConfig()
		{
			ProcessDynamicConfig();
		}

        private void ProcessStaticConfig()
        {
			// at startup only
            var staticSection = configRoot.GetSection("static");
            var clientSection = staticSection.GetSection("client");
			ConfigurationData.staticProperties.client.config_tend_count = GetInt(clientSection, "config_tend_count");
			this.Interval = ConfigurationData.staticProperties.client.config_tend_count;
            ConfigurationData.staticProperties.client.max_connections_per_node = GetInt(clientSection, "max_connections_per_node");
            ConfigurationData.staticProperties.client.min_connections_per_node = GetInt(clientSection, "min_connections_per_node");
            ConfigurationData.staticProperties.client.async_max_connections_per_node = GetInt(clientSection, "async_max_connections_per_node");
            ConfigurationData.staticProperties.client.async_min_connections_per_node = GetInt(clientSection, "async_min_connections_per_node");
		}

        private void ProcessDynamicConfig()
        {
			// if file changed
            
            var metaDataSection = configRoot.GetSection("metadata");
			ConfigurationData.metaData.version = metaDataSection.GetSection("version").Value;
            ConfigurationData.metaData.app_name = metaDataSection.GetSection("app_name").Value;
            ConfigurationData.metaData.generation = GetInt(metaDataSection, "generation");

			var dynamicSection = configRoot.GetSection("dynamic");
            var clientSection = dynamicSection.GetSection("client");
			ConfigurationData.dynamicProperties.client.timeout = GetInt(clientSection, "timeout");
            ConfigurationData.dynamicProperties.client.error_rate_window = GetInt(clientSection, "error_rate_window");
            ConfigurationData.dynamicProperties.client.max_error_rate = GetInt(clientSection, "max_error_rate");
            ConfigurationData.dynamicProperties.client.fail_if_not_connected = GetBool(clientSection, "fail_if_not_connected");
            ConfigurationData.dynamicProperties.client.login_timeout = GetInt(clientSection, "login_timeout");
            ConfigurationData.dynamicProperties.client.max_socket_idle = GetInt(clientSection, "max_socket_idle");
            ConfigurationData.dynamicProperties.client.rack_aware = GetBool(clientSection, "rack_aware");
            ConfigurationData.dynamicProperties.client.rack_ids = GetIntArray(clientSection, "rack_ids");
            ConfigurationData.dynamicProperties.client.tend_interval = GetInt(clientSection, "tend_interval");
            ConfigurationData.dynamicProperties.client.use_service_alternative = GetBool(clientSection, "use_service_alternative");

			var read = dynamicSection.GetSection("read");
            ConfigurationData.dynamicProperties.read.read_mode_ap = GetReadModeAP(read);
            ConfigurationData.dynamicProperties.read.read_mode_sc = GetReadModeSC(read);
            ConfigurationData.dynamicProperties.read.connect_timeout = GetInt(read, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.read.fail_on_filtered_out = GetBool(read, FAIL_ON_FILTERED_OUT);
            ConfigurationData.dynamicProperties.read.replica = GetReplica(read);
            ConfigurationData.dynamicProperties.read.sleep_between_retries = GetInt(read, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.read.socket_timeout = GetInt(read, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.read.timeout_delay = GetInt(read, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.read.max_retries = GetInt(read, MAX_RETRIES);
			

			var write = dynamicSection.GetSection("write");
            ConfigurationData.dynamicProperties.write.fail_on_filtered_out = GetBool(write, FAIL_ON_FILTERED_OUT);
            ConfigurationData.dynamicProperties.write.replica = GetReplica(write);
            ConfigurationData.dynamicProperties.write.send_key = GetBool(write, SEND_KEY);
            ConfigurationData.dynamicProperties.write.sleep_between_retries = GetInt(write, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.write.socket_timeout = GetInt(write, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.write.timeout_delay = GetInt(write, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.write.max_retries = GetInt(write, MAX_RETRIES);
            ConfigurationData.dynamicProperties.write.durable_delete = GetBool(write, DURABLE_DELETE);

			var query = dynamicSection.GetSection("query");
            ConfigurationData.dynamicProperties.query.read_mode_ap = GetReadModeAP(query);
            ConfigurationData.dynamicProperties.query.read_mode_sc = GetReadModeSC(query);
            ConfigurationData.dynamicProperties.query.connect_timeout = GetInt(query, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.query.replica = GetReplica(query);
            ConfigurationData.dynamicProperties.query.sleep_between_retries = GetInt(query, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.query.socket_timeout = GetInt(query, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.query.timeout_delay = GetInt(query, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.query.max_retries = GetInt(query, MAX_RETRIES);
            ConfigurationData.dynamicProperties.query.include_bin_data = GetBool(query, "include_bin_data");
            ConfigurationData.dynamicProperties.query.info_timeout = GetInt(query, "info_timeout");
            ConfigurationData.dynamicProperties.query.expected_duration = GetQueryDuration(query);

            var scan = dynamicSection.GetSection("scan");
            ConfigurationData.dynamicProperties.scan.read_mode_ap = GetReadModeAP(scan);
            ConfigurationData.dynamicProperties.scan.read_mode_sc = GetReadModeSC(scan);
            ConfigurationData.dynamicProperties.scan.connect_timeout = GetInt(scan, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.scan.replica = GetReplica(scan);
            ConfigurationData.dynamicProperties.scan.sleep_between_retries = GetInt(scan, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.scan.socket_timeout = GetInt(scan, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.scan.timeout_delay = GetInt(scan, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.scan.max_retries = GetInt(scan, MAX_RETRIES);
            ConfigurationData.dynamicProperties.scan.concurrent_nodes = GetBool(scan, "concurrent_nodes");
            ConfigurationData.dynamicProperties.scan.max_concurrent_nodes = GetInt(scan, "max_concurrent_nodes");

			var batch_read = dynamicSection.GetSection("batch_read");
            ConfigurationData.dynamicProperties.batch_read.read_mode_ap = GetReadModeAP(batch_read);
            ConfigurationData.dynamicProperties.batch_read.read_mode_sc = GetReadModeSC(batch_read);
            ConfigurationData.dynamicProperties.batch_read.connect_timeout = GetInt(batch_read, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.batch_read.replica = GetReplica(batch_read);
            ConfigurationData.dynamicProperties.batch_read.sleep_between_retries = GetInt(batch_read, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.batch_read.socket_timeout = GetInt(batch_read, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.batch_read.timeout_delay = GetInt(batch_read, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.batch_read.max_concurrent_threads = GetInt(batch_read, MAX_CONCURRENT_THREADS);
            ConfigurationData.dynamicProperties.batch_read.allow_inline = GetBool(batch_read, ALLOW_INLINE);
            ConfigurationData.dynamicProperties.batch_read.allow_inline_ssd = GetBool(batch_read, ALLOW_INLINE_SSD);
            ConfigurationData.dynamicProperties.batch_read.respond_all_keys = GetBool(batch_read, RESPOND_ALL_KEYS);
			 
			var batch_write = dynamicSection.GetSection("batch_write");
            ConfigurationData.dynamicProperties.batch_write.connect_timeout = GetInt(batch_write, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.batch_write.fail_on_filtered_out = GetBool(batch_write, FAIL_ON_FILTERED_OUT);
            ConfigurationData.dynamicProperties.batch_write.replica = GetReplica(batch_write);
            ConfigurationData.dynamicProperties.batch_write.sleep_between_retries = GetInt(batch_write, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.batch_write.socket_timeout = GetInt(batch_write, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.batch_write.timeout_delay = GetInt(batch_write, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.batch_write.max_retries = GetInt(batch_write, MAX_RETRIES);
            ConfigurationData.dynamicProperties.batch_write.durable_delete = GetBool(batch_write, DURABLE_DELETE);
            ConfigurationData.dynamicProperties.batch_write.send_key = GetBool(batch_write, SEND_KEY);
            ConfigurationData.dynamicProperties.batch_write.max_concurrent_threads = GetInt(batch_write, MAX_CONCURRENT_THREADS);
            ConfigurationData.dynamicProperties.batch_write.allow_inline = GetBool(batch_write, ALLOW_INLINE);
            ConfigurationData.dynamicProperties.batch_write.allow_inline_ssd = GetBool(batch_write, ALLOW_INLINE_SSD);
            ConfigurationData.dynamicProperties.batch_write.respond_all_keys = GetBool(batch_write, RESPOND_ALL_KEYS);

			var batch_udf = dynamicSection.GetSection("batch_udf");
            ConfigurationData.dynamicProperties.batch_udf.durable_delete = GetBool(batch_udf, DURABLE_DELETE);
            ConfigurationData.dynamicProperties.batch_udf.send_key = GetBool(batch_udf, SEND_KEY);

			var batch_delete = dynamicSection.GetSection("batch_delete");
            ConfigurationData.dynamicProperties.batch_delete.durable_delete = GetBool(batch_delete, DURABLE_DELETE);
            ConfigurationData.dynamicProperties.batch_delete.send_key = GetBool(batch_delete, SEND_KEY);

			var txn_roll = dynamicSection.GetSection("txn_roll");
            ConfigurationData.dynamicProperties.txn_roll.read_mode_ap = GetReadModeAP(txn_roll);
            ConfigurationData.dynamicProperties.txn_roll.read_mode_sc = GetReadModeSC(txn_roll);
            ConfigurationData.dynamicProperties.txn_roll.connect_timeout = GetInt(txn_roll, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_roll.replica = GetReplica(txn_roll);
            ConfigurationData.dynamicProperties.txn_roll.sleep_between_retries = GetInt(txn_roll, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.txn_roll.socket_timeout = GetInt(txn_roll, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_roll.timeout_delay = GetInt(txn_roll, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.txn_roll.total_timeout = GetInt(txn_roll, TOTAL_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_roll.max_retries = GetInt(txn_roll, MAX_RETRIES);
            ConfigurationData.dynamicProperties.txn_roll.max_concurrent_threads = GetInt(txn_roll, MAX_CONCURRENT_THREADS);
            ConfigurationData.dynamicProperties.txn_roll.allow_inline = GetBool(txn_roll, ALLOW_INLINE);
            ConfigurationData.dynamicProperties.txn_roll.allow_inline_ssd = GetBool(txn_roll, ALLOW_INLINE_SSD);
            ConfigurationData.dynamicProperties.txn_roll.respond_all_keys = GetBool(txn_roll, RESPOND_ALL_KEYS);

			var txn_verify = dynamicSection.GetSection("txn_verify");
            ConfigurationData.dynamicProperties.txn_verify.read_mode_ap = GetReadModeAP(txn_verify);
            ConfigurationData.dynamicProperties.txn_verify.read_mode_sc = GetReadModeSC(txn_verify);
            ConfigurationData.dynamicProperties.txn_verify.connect_timeout = GetInt(txn_verify, CONNECT_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_verify.replica = GetReplica(txn_verify);
            ConfigurationData.dynamicProperties.txn_verify.sleep_between_retries = GetInt(txn_verify, SLEEP_BETWEEN_RETRIES);
            ConfigurationData.dynamicProperties.txn_verify.socket_timeout = GetInt(txn_verify, SOCKET_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_verify.timeout_delay = GetInt(txn_verify, TIMEOUT_DLEAY);
            ConfigurationData.dynamicProperties.txn_verify.total_timeout = GetInt(txn_verify, TOTAL_TIMEOUT);
            ConfigurationData.dynamicProperties.txn_verify.max_retries = GetInt(txn_verify, MAX_RETRIES);
            ConfigurationData.dynamicProperties.txn_verify.max_concurrent_threads = GetInt(txn_verify, MAX_CONCURRENT_THREADS);
            ConfigurationData.dynamicProperties.txn_verify.allow_inline = GetBool(txn_verify, ALLOW_INLINE);
            ConfigurationData.dynamicProperties.txn_verify.allow_inline_ssd = GetBool(txn_verify, ALLOW_INLINE_SSD);
            ConfigurationData.dynamicProperties.txn_verify.respond_all_keys = GetBool(txn_verify, RESPOND_ALL_KEYS);
		}

		private static int GetInt(IConfigurationSection section, string propertyName)
		{
			string value = section.GetSection(propertyName).Value;
			if (value != null)
			{
				return Int32.Parse(value);
			}
			return 0;
		}

		private static bool GetBool(IConfigurationSection section, string propertyName)
		{
			string value = section.GetSection(propertyName).Value;
			if (value != null)
			{
				return Boolean.Parse(value);
			}
			return false;
		}

		private static int[] GetIntArray(IConfigurationSection section, string propertyName)
		{
			var arraySection = section.GetSection(propertyName);
			int count = arraySection.GetChildren().Count();
			var integers = new int[count];
			if (count > 0)
			{
				for (int i=0; i<count; i++)
				{
					integers[i] = Int32.Parse(arraySection.GetSection(i.ToString()).Value);
				}
			}

			return integers;
		}

		private static ReadModeAP GetReadModeAP(IConfigurationSection section)
		{
			string value = section.GetSection(READ_MODE_AP).Value;
			if (value != null)
			{
				return value switch
				{
					"ONE" => ReadModeAP.ONE,
					"ALL" => ReadModeAP.ALL,
					_ => ReadModeAP.ONE,
				};
			}
			return ReadModeAP.ONE;
		}

		private static ReadModeSC GetReadModeSC(IConfigurationSection section)
		{
			string value = section.GetSection(READ_MODE_SC).Value;
			if (value != null)
			{
				return value switch
				{
					"SESSION" => ReadModeSC.SESSION,
					"LINEARIZE" => ReadModeSC.LINEARIZE,
					"ALLOW_REPLICA" => ReadModeSC.ALLOW_REPLICA,
					"ALLOW_UNAVAILABLE" => ReadModeSC.ALLOW_UNAVAILABLE,
					_ => ReadModeSC.SESSION
				};
			}
			return ReadModeSC.SESSION;
		}

		private static Replica GetReplica(IConfigurationSection section)
		{
			string value = section.GetSection(REPLICA).Value;
			if (value != null)
			{
				return value switch
				{
					"MASTER" => Replica.MASTER,
					"MASTER_PROLES" => Replica.MASTER_PROLES,
					"SEQUENCE" => Replica.SEQUENCE,
					"PREFER_RACK" => Replica.PREFER_RACK,
					"RANDOM" => Replica.RANDOM,
					_ => Replica.MASTER
				};
			}
			return Replica.MASTER;
		}

		private static QueryDuration GetQueryDuration(IConfigurationSection section)
		{
			string value = section.GetSection("expected_duration").Value;
			if (value != null)
			{
				return value switch
				{
					"LONG" => QueryDuration.LONG,
					"SHORT" => QueryDuration.SHORT,
					"LONG_RELAX_AP" => QueryDuration.LONG_RELAX_AP,
					_ => QueryDuration.LONG
				};
			}
			return QueryDuration.LONG;
		}
	}
}
