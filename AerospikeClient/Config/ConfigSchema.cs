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

namespace Aerospike.Client.Config
{
	public class ConfigurationData
	{
		public ConfigurationData() { }

		public StaticConfig staticConfig { get; set; }
		public DynamicConfig dynamicConfig { get; set; }
	}

	public class StaticConfig
	{
		public StaticConfig()
		{
			client = new StaticClientConfig();
		}

		public StaticClientConfig client { get; set; }
	}

	public class StaticClientConfig
	{
		public int? config_interval { get; set; }
		public int? max_connections_per_node { get; set; }
		public int? min_connections_per_node { get; set; }
		public int? async_max_connections_per_node { get; set; }
		public int? async_min_connections_per_node { get; set; }
	}

	public class DynamicConfig
	{
		public DynamicConfig()
		{
			client = new DynamicClientConfig();
			read = new ReadConfig();
			write = new WriteConfig();
			query = new QueryConfig();
			scan = new ScanConfig();
			batch_read = new BatchReadConfig();
			batch_write = new BatchWriteConfig();
			batch_udf = new BatchUDFDeleteConfig();
			batch_delete = new BatchUDFDeleteConfig();
			txn_roll = new TxnRollConfig();
			txn_verify = new TxnVerifyConfig();
		}

		public DynamicClientConfig client { get; set; }
		public ReadConfig read { get; set; }
		public WriteConfig write { get; set; }
		public QueryConfig query { get; set; }
		public ScanConfig scan { get; set; }
		public BatchReadConfig batch_read { get; set; }
		public BatchWriteConfig batch_write { get; set; }
		public BatchUDFDeleteConfig batch_udf { get; set; }
		public BatchUDFDeleteConfig batch_delete { get; set; }
		public TxnRollConfig txn_roll { get; set; }
		public TxnVerifyConfig txn_verify { get; set; }
	}

	public class DynamicClientConfig
	{
		public int? timeout { get; set; }
		public int? error_rate_window { get; set; }
		public int? max_error_rate { get; set; }
		public bool? fail_if_not_connected { get; set; }
		public int? login_timeout { get; set; }
		public int? max_socket_idle { get; set; }
		public bool? rack_aware { get; set; }
		public int[] rack_ids { get; set; }
		public int? tend_interval { get; set; }
		public bool? use_service_alternative { get; set; }
	}

	public class ReadConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public bool? fail_on_filtered_out { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
	}

	public class WriteConfig
	{
		public int? connect_timeout { get; set; }
		public bool? fail_on_filtered_out { get; set; }
		public Replica? replica { get; set; }
		public bool? send_key { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public bool? durable_delete { get; set; }
	}

	public class QueryConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public bool? include_bin_data { get; set; }
		public int? info_timeout { get; set; }
		public int? record_queue_size { get; set; }
		public QueryDuration? expected_duration { get; set; }
	}

	public class ScanConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public bool? concurrent_nodes { get; set; }
		public int? max_concurrent_nodes { get; set; }
	}

	public class BatchReadConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public int? max_concurrent_threads { get; set; }
		public bool? allow_inline { get; set; }
		public bool? allow_inline_ssd { get; set; }
		public bool? respond_all_keys { get; set; }
	}

	public class BatchWriteConfig
	{
		public int? connect_timeout { get; set; }
		public bool? fail_on_filtered_out { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public bool? durable_delete { get; set; }
		public bool? send_key { get; set; }
		public int? max_concurrent_threads { get; set; }
		public bool? allow_inline { get; set; }
		public bool? allow_inline_ssd { get; set; }
		public bool? respond_all_keys { get; set; }
	}

	public class BatchUDFDeleteConfig
	{
		public bool? durable_delete { get; set; }
		public bool? send_key { get; set; }
	}

	public class TxnRollConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public int? max_concurrent_threads { get; set; }
		public bool? allow_inline { get; set; }
		public bool? allow_inline_ssd { get; set; }
		public bool? respond_all_keys { get; set; }
	}

	public class TxnVerifyConfig
	{
		public ReadModeAP? read_mode_ap { get; set; }
		public ReadModeSC? read_mode_sc { get; set; }
		public int? connect_timeout { get; set; }
		public Replica? replica { get; set; }
		public int? sleep_between_retries { get; set; }
		public int? socket_timeout { get; set; }
		public int? timeout_delay { get; set; }
		public int? total_timeout { get; set; }
		public int? max_retries { get; set; }
		public int? max_concurrent_threads { get; set; }
		public bool? allow_inline { get; set; }
		public bool? allow_inline_ssd { get; set; }
		public bool? respond_all_keys { get; set; }
	}
}
