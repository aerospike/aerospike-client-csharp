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

        public int Interval { get; private set; }

        private IConfigurationRoot config;

        public MetaData MetaData { get; private set; }

        public StaticProperties StaticProperties { get; private set; }

        public DynamicProperties DynamicProperties { get; private set; }

        public string YamlFilePath;

		public AerospikeYamlConfigProvider()
        {
            YamlFilePath = "." + Path.DirectorySeparatorChar + "aerospikeconfig.yaml";
			config = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
                .Build();
            MetaData = new MetaData();
            StaticProperties = new StaticProperties();
            DynamicProperties = new DynamicProperties();
			config.GetSection("metadata").Bind(MetaData);
            config.GetSection("static").Bind(StaticProperties);
            config.GetSection("dynamic").Bind(DynamicProperties);
        }

		public AerospikeYamlConfigProvider(string path)
        {
            YamlFilePath = path;
			config = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
				.Build();
			MetaData = new MetaData();
			StaticProperties = new StaticProperties();
            DynamicProperties = new DynamicProperties();
            config.GetSection("metadata").Bind(MetaData);
            config.GetSection("static").Bind(StaticProperties);
            config.GetSection("dynamic").Bind(DynamicProperties);
        }

        public void Watch()
        {
            _ = this.config.GetReloadToken().RegisterChangeCallback(_ => {
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
            var staticSection = config.GetSection("static");
            var clientSection = staticSection.GetSection("client");
			StaticProperties.client.config_tend_count = GetInt(clientSection, "config_tend_count");
			this.Interval = StaticProperties.client.config_tend_count;
			StaticProperties.client.max_connections_per_node = GetInt(clientSection, "max_connections_per_node");
			StaticProperties.client.min_connections_per_node = GetInt(clientSection, "min_connections_per_node");
			StaticProperties.client.async_max_connections_per_node = GetInt(clientSection, "async_max_connections_per_node");
			StaticProperties.client.async_min_connections_per_node = GetInt(clientSection, "async_min_connections_per_node");
		}

        private void ProcessDynamicConfig()
        {
			// if file changed
            
            var metaDataSection = config.GetSection("metadata");
			MetaData.version = metaDataSection.GetSection("version").Value;
			MetaData.app_name = metaDataSection.GetSection("app_name").Value;
			MetaData.generation = GetInt(metaDataSection, "generation");

			var dynamicSection = config.GetSection("dynamic");
            var clientSection = dynamicSection.GetSection("client");
			DynamicProperties.client.timeout = GetInt(clientSection, "timeout");
			DynamicProperties.client.error_rate_window = GetInt(clientSection, "error_rate_window");
			DynamicProperties.client.max_error_rate = GetInt(clientSection, "max_error_rate");
			DynamicProperties.client.fail_if_not_connected = GetBool(clientSection, "fail_if_not_connected");
			DynamicProperties.client.login_timeout = GetInt(clientSection, "login_timeout");
			DynamicProperties.client.max_socket_idle = GetInt(clientSection, "max_socket_idle");
			DynamicProperties.client.rack_aware = GetBool(clientSection, "rack_aware");
			DynamicProperties.client.rack_ids = GetIntArray(clientSection, "rack_ids");
			DynamicProperties.client.tend_interval = GetInt(clientSection, "tend_interval");
			DynamicProperties.client.use_service_alternative = GetBool(clientSection, "use_service_alternative");

			var read = dynamicSection.GetSection("read");
			DynamicProperties.read.read_mode_ap = GetReadModeAP(read);
			DynamicProperties.read.read_mode_sc = GetReadModeSC(read);
			DynamicProperties.read.connect_timeout = GetInt(read, CONNECT_TIMEOUT);
			DynamicProperties.read.fail_on_filtered_out = GetBool(read, FAIL_ON_FILTERED_OUT);
			DynamicProperties.read.replica = GetReplica(read);
			DynamicProperties.read.sleep_between_retries = GetInt(read, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.read.socket_timeout = GetInt(read, SOCKET_TIMEOUT);
			DynamicProperties.read.timeout_delay = GetInt(read, TIMEOUT_DLEAY);
			DynamicProperties.read.max_retries = GetInt(read, MAX_RETRIES);
			

			var write = dynamicSection.GetSection("write");
			DynamicProperties.write.connect_timeout = GetInt(write, CONNECT_TIMEOUT);
			DynamicProperties.write.fail_on_filtered_out = GetBool(write, FAIL_ON_FILTERED_OUT);
			DynamicProperties.write.replica = GetReplica(write);
			DynamicProperties.write.send_key = GetBool(write, SEND_KEY);
			DynamicProperties.write.sleep_between_retries = GetInt(write, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.write.socket_timeout = GetInt(write, SOCKET_TIMEOUT);
			DynamicProperties.write.timeout_delay = GetInt(write, TIMEOUT_DLEAY);
			DynamicProperties.write.max_retries = GetInt(write, MAX_RETRIES);
			DynamicProperties.write.durable_delete = GetBool(write, DURABLE_DELETE);

			var query = dynamicSection.GetSection("query");
			DynamicProperties.query.read_mode_ap = GetReadModeAP(query);
			DynamicProperties.query.read_mode_sc = GetReadModeSC(query);
			DynamicProperties.query.connect_timeout = GetInt(query, CONNECT_TIMEOUT);
			DynamicProperties.query.replica = GetReplica(query);
			DynamicProperties.query.sleep_between_retries = GetInt(query, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.query.socket_timeout = GetInt(query, SOCKET_TIMEOUT);
			DynamicProperties.query.timeout_delay = GetInt(query, TIMEOUT_DLEAY);
			DynamicProperties.query.max_retries = GetInt(query, MAX_RETRIES);
			DynamicProperties.query.include_bin_data = GetBool(query, "include_bin_data");
			DynamicProperties.query.info_timeout = GetInt(query, "info_timeout");
			DynamicProperties.query.expected_duration = GetQueryDuration(query);

            var scan = dynamicSection.GetSection("scan");
			DynamicProperties.scan.read_mode_ap = GetReadModeAP(scan);
			DynamicProperties.scan.read_mode_sc = GetReadModeSC(scan);
			DynamicProperties.scan.connect_timeout = GetInt(scan, CONNECT_TIMEOUT);
			DynamicProperties.scan.replica = GetReplica(scan);
			DynamicProperties.scan.sleep_between_retries = GetInt(scan, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.scan.socket_timeout = GetInt(scan, SOCKET_TIMEOUT);
			DynamicProperties.scan.timeout_delay = GetInt(scan, TIMEOUT_DLEAY);
			DynamicProperties.scan.max_retries = GetInt(scan, MAX_RETRIES);
			DynamicProperties.scan.concurrent_nodes = GetBool(scan, "concurrent_nodes");
			DynamicProperties.scan.max_concurrent_nodes = GetInt(scan, "max_concurrent_nodes");

			var batch_read = dynamicSection.GetSection("batch_read");
			DynamicProperties.batch_read.read_mode_ap = GetReadModeAP(batch_read);
			DynamicProperties.batch_read.read_mode_sc = GetReadModeSC(batch_read);
			DynamicProperties.batch_read.connect_timeout = GetInt(batch_read, CONNECT_TIMEOUT);
			DynamicProperties.batch_read.replica = GetReplica(batch_read);
			DynamicProperties.batch_read.sleep_between_retries = GetInt(batch_read, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.batch_read.socket_timeout = GetInt(batch_read, SOCKET_TIMEOUT);
			DynamicProperties.batch_read.timeout_delay = GetInt(batch_read, TIMEOUT_DLEAY);
			DynamicProperties.batch_read.max_concurrent_threads = GetInt(batch_read, MAX_CONCURRENT_THREADS);
			DynamicProperties.batch_read.allow_inline = GetBool(batch_read, ALLOW_INLINE);
			DynamicProperties.batch_read.allow_inline_ssd = GetBool(batch_read, ALLOW_INLINE_SSD);
			DynamicProperties.batch_read.respond_all_keys = GetBool(batch_read, RESPOND_ALL_KEYS);
			 
			var batch_write = dynamicSection.GetSection("batch_write");
			DynamicProperties.batch_write.connect_timeout = GetInt(batch_write, CONNECT_TIMEOUT);
			DynamicProperties.batch_write.fail_on_filtered_out = GetBool(batch_write, FAIL_ON_FILTERED_OUT);
			DynamicProperties.batch_write.replica = GetReplica(batch_write);
			DynamicProperties.batch_write.sleep_between_retries = GetInt(batch_write, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.batch_write.socket_timeout = GetInt(batch_write, SOCKET_TIMEOUT);
			DynamicProperties.batch_write.timeout_delay = GetInt(batch_write, TIMEOUT_DLEAY);
			DynamicProperties.batch_write.max_retries = GetInt(batch_write, MAX_RETRIES);
			DynamicProperties.batch_write.durable_delete = GetBool(batch_write, DURABLE_DELETE);
			DynamicProperties.batch_write.send_key = GetBool(batch_write, SEND_KEY);
			DynamicProperties.batch_write.max_concurrent_threads = GetInt(batch_write, MAX_CONCURRENT_THREADS);
			DynamicProperties.batch_write.allow_inline = GetBool(batch_write, ALLOW_INLINE);
			DynamicProperties.batch_write.allow_inline_ssd = GetBool(batch_write, ALLOW_INLINE_SSD);
			DynamicProperties.batch_write.respond_all_keys = GetBool(batch_write, RESPOND_ALL_KEYS);

			var batch_udf = dynamicSection.GetSection("batch_udf");
			DynamicProperties.batch_udf.durable_delete = GetBool(batch_udf, DURABLE_DELETE);
			DynamicProperties.batch_udf.send_key = GetBool(batch_udf, SEND_KEY);

			var batch_delete = dynamicSection.GetSection("batch_delete");
			DynamicProperties.batch_delete.durable_delete = GetBool(batch_delete, DURABLE_DELETE);
			DynamicProperties.batch_delete.send_key = GetBool(batch_delete, SEND_KEY);

			var txn_roll = dynamicSection.GetSection("txn_roll");
			DynamicProperties.txn_roll.read_mode_ap = GetReadModeAP(txn_roll);
			DynamicProperties.txn_roll.read_mode_sc = GetReadModeSC(txn_roll);
			DynamicProperties.txn_roll.connect_timeout = GetInt(txn_roll, CONNECT_TIMEOUT);
			DynamicProperties.txn_roll.replica = GetReplica(txn_roll);
			DynamicProperties.txn_roll.sleep_between_retries = GetInt(txn_roll, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.txn_roll.socket_timeout = GetInt(txn_roll, SOCKET_TIMEOUT);
			DynamicProperties.txn_roll.timeout_delay = GetInt(txn_roll, TIMEOUT_DLEAY);
			DynamicProperties.txn_roll.total_timeout = GetInt(txn_roll, TOTAL_TIMEOUT);
			DynamicProperties.txn_roll.max_retries = GetInt(txn_roll, MAX_RETRIES);
			DynamicProperties.txn_roll.max_concurrent_threads = GetInt(txn_roll, MAX_CONCURRENT_THREADS);
			DynamicProperties.txn_roll.allow_inline = GetBool(txn_roll, ALLOW_INLINE);
			DynamicProperties.txn_roll.allow_inline_ssd = GetBool(txn_roll, ALLOW_INLINE_SSD);
			DynamicProperties.txn_roll.respond_all_keys = GetBool(txn_roll, RESPOND_ALL_KEYS);

			var txn_verify = dynamicSection.GetSection("txn_verify");
			DynamicProperties.txn_verify.read_mode_ap = GetReadModeAP(txn_verify);
			DynamicProperties.txn_verify.read_mode_sc = GetReadModeSC(txn_verify);
			DynamicProperties.txn_verify.connect_timeout = GetInt(txn_verify, CONNECT_TIMEOUT);
			DynamicProperties.txn_verify.replica = GetReplica(txn_verify);
			DynamicProperties.txn_verify.sleep_between_retries = GetInt(txn_verify, SLEEP_BETWEEN_RETRIES);
			DynamicProperties.txn_verify.socket_timeout = GetInt(txn_verify, SOCKET_TIMEOUT);
			DynamicProperties.txn_verify.timeout_delay = GetInt(txn_verify, TIMEOUT_DLEAY);
			DynamicProperties.txn_verify.total_timeout = GetInt(txn_verify, TOTAL_TIMEOUT);
			DynamicProperties.txn_verify.max_retries = GetInt(txn_verify, MAX_RETRIES);
			DynamicProperties.txn_verify.max_concurrent_threads = GetInt(txn_verify, MAX_CONCURRENT_THREADS);
			DynamicProperties.txn_verify.allow_inline = GetBool(txn_verify, ALLOW_INLINE);
			DynamicProperties.txn_verify.allow_inline_ssd = GetBool(txn_verify, ALLOW_INLINE_SSD);
			DynamicProperties.txn_verify.respond_all_keys = GetBool(txn_verify, RESPOND_ALL_KEYS);
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
