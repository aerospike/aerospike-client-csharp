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

		public MetaData metaData { get; set; }
		public StaticProperties staticProperties { get; set; }
		public DynamicProperties dynamicProperties { get; set; }
	}
	
	public class MetaData
	{
		public MetaData()
		{

		}
		
		public string version { get; set; }
		public string app_name { get; set; }
		public int? generation { get; set; }
	}

	public class StaticProperties
	{
		public StaticProperties()
		{
			client = new StaticClient();
		}

		public StaticClient client { get; set; }
	}

	public class StaticClient
	{
		public int? config_tend_count { get; set; }
		public int? max_connections_per_node { get; set; }
		public int? min_connections_per_node { get; set; }
		public int? async_max_connections_per_node { get; set; }
		public int? async_min_connections_per_node { get; set; }
	}

	public class DynamicProperties
	{
		public DynamicProperties()
		{
			client = new DynamicClient();
			read = new ReadProperties();
			write = new WriteProperties();
			query = new QueryProperties();
			scan = new ScanProperties();
			batch_read = new BatchReadProperties();
			batch_write = new BatchWriteProperties();
			batch_udf = new BatchUDFDeleteProperties();
			batch_delete = new BatchUDFDeleteProperties();
			txn_roll = new TxnRollProperties();
			txn_verify = new TxnVerifyProperties();
		}

		public DynamicClient client { get; set; }
		public ReadProperties read { get; set; }
		public WriteProperties write { get; set; }
		public QueryProperties query { get; set; }
		public ScanProperties scan { get; set; }
		public BatchReadProperties batch_read { get; set; }
		public BatchWriteProperties batch_write { get; set; }
		public BatchUDFDeleteProperties batch_udf { get; set; }
		public BatchUDFDeleteProperties batch_delete { get; set; }
		public TxnRollProperties txn_roll { get; set; }
		public TxnVerifyProperties txn_verify { get; set; }

		public MetricsProperties metrics { get; set; }
	}

	public class DynamicClient
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

	public class ReadProperties
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

	public class WriteProperties
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

	public class QueryProperties
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

	public class ScanProperties
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

	public class BatchReadProperties
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

	public class BatchWriteProperties
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

	public class BatchUDFDeleteProperties
	{
		public bool? durable_delete { get; set; }
		public bool? send_key { get; set; }
	}

	public class TxnRollProperties
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

	public class TxnVerifyProperties
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

	public class MetricsProperties
	{
		public bool? enable { get; set; }
		public int? latency_shift { get; set; }
		public int? latency_columns { get; set; }
	}
}
