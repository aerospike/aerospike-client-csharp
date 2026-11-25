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

namespace Aerospike.Client.Config
{
	/// <summary>
	/// Marks the dynamic configuration schema version where a property was introduced.
	/// </summary>
	[AttributeUsage(AttributeTargets.Property)]
	public sealed class ConfigIntroducedInAttribute : Attribute
	{
		public ConfigIntroducedInAttribute(string version)
		{
			Version = new Version(version);
		}

		public Version Version { get; }
	}

	public interface IConfigurationData
	{
		public string version { get; set; }
		public StaticConfig staticConfig { get; set; }
		public DynamicConfig dynamicConfig { get; set; }
	}

	public class ConfigurationDatav1_0_0 : IConfigurationData
	{
		public ConfigurationDatav1_0_0()
		{
			staticConfig = new StaticConfig();
			dynamicConfig = new DynamicConfig();
		}

		public string version { get; set; }
		public StaticConfig staticConfig { get; set; }
		public DynamicConfig dynamicConfig { get; set; }
	}

	public class ConfigurationDatav1_1_0 : ConfigurationDatav1_0_0
	{
		public ConfigurationDatav1_1_0()
		{
			version = "1.1.0";
		}
	}

	public class ConfigurationDatav1_2_0 : ConfigurationDatav1_1_0
	{
		public ConfigurationDatav1_2_0()
		{
			version = "1.2.0";
		}
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

		[ConfigIntroducedIn("1.2.0")]
		public Dictionary<string, string> ip_map { get; set; }

		[ConfigIntroducedIn("1.2.0")]
		public bool? use_services_alternate { get; set; }

		[ConfigIntroducedIn("1.2.0")]
		public StaticDiscoveryConfig discovery { get; set; }

		[ConfigIntroducedIn("1.2.0")]
		public StaticTranslationConfig translation { get; set; }
	}

	/// <summary>
	/// Settings under <c>static.client.discovery</c> (schema 1.2.0+).
	/// </summary>
	public class StaticDiscoveryConfig
	{
		public const string Static = "static";
		public const string Dns = "dns";
		public const string AwsTags = "aws-tags";

		public string provider { get; set; }
		public string merge_policy { get; set; }
		public int? refresh_interval_ms { get; set; }
		public int? tend_refresh_deadline_ms { get; set; }
		public string execution_mode { get; set; }
		public int? refresh_timeout_ms { get; set; }
		public DnsDiscoveryConfig dns { get; set; }
		public AwsDiscoveryConfig aws { get; set; }

		/// <summary>
		/// Options for a registered provider (every key of the block except <c>provider</c>).
		/// Null for built-in providers.
		/// </summary>
		public Dictionary<string, object> options { get; set; }
	}

	/// <summary>
	/// Settings under <c>static.client.discovery.dns</c>.
	/// </summary>
	public class DnsDiscoveryConfig
	{
		public string[] names { get; set; }
		public string record_type { get; set; }
		public int? port { get; set; }
		public string tls_name { get; set; }
		public int? timeout_ms { get; set; }
		public string[] servers { get; set; }
	}

	/// <summary>
	/// Settings under <c>static.client.discovery.aws</c>.
	/// </summary>
	public class AwsDiscoveryConfig
	{
		public string region { get; set; }
		public AwsDiscoveryFilterConfig[] filters { get; set; }
		public string address_field { get; set; }
		public int? port { get; set; }
		public string tls_name { get; set; }
		public int? timeout_ms { get; set; }
		public int? init_timeout_ms { get; set; }
		public string endpoint { get; set; }
	}

	/// <summary>
	/// One entry under <c>static.client.discovery.aws.filters</c>.
	/// </summary>
	public class AwsDiscoveryFilterConfig
	{
		public string name { get; set; }
		public string[] values { get; set; }
	}

	/// <summary>
	/// Settings under <c>static.client.translation</c> (schema 1.2.0+).
	/// </summary>
	public class StaticTranslationConfig
	{
		public const string StaticMap = "static-map";
		public const string PortMap = "port-map";
		public const string PassThrough = "pass-through";

		public string provider { get; set; }
		public string on_miss { get; set; }
		public Dictionary<string, string> static_map { get; set; }
		public Dictionary<string, string> port_map { get; set; }

		/// <summary>
		/// Options for a registered translator (every key of the block except <c>provider</c>).
		/// Null for built-in providers.
		/// </summary>
		public Dictionary<string, object> options { get; set; }
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
			metrics = new MetricsConfig();
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
		public MetricsConfig metrics { get; set; }
	}

	public class DynamicClientConfig
	{
		public string app_id { get; set; }
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
		[ConfigIntroducedIn("1.1.0")]
		public int? error_detail_verbosity { get; set; }
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
		[ConfigIntroducedIn("1.1.0")]
		public int? error_detail_verbosity { get; set; }
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

	public class MetricsConfig
	{
		public bool? enable { get; set; }
		public int? latency_shift { get; set; }
		public int? latency_columns { get; set; }
		public Dictionary<string, string> labels { get; set; }

	}
}
