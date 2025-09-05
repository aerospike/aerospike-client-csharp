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
	public class YamlConfigProvider : IConfigProvider
	{
		//-------------------------------------------------------
		// Constants.
		//-------------------------------------------------------

		private const string CONFIG_PATH_ENV = "AEROSPIKE_CLIENT_CONFIG_URL";

		private const string DEFAULT_CONFIG_URL_PREFIX = "file://";

		#region YAML Section Names

		private const string staticClientName = "static.client";
		private const string dynamicClientName = "dynamic.client";
		private const string dynamicReadName = "dynamic.read";
		private const string dynamicWriteName = "dynamic.write";
		private const string dynamicQueryName = "dynamic.query";
		private const string dynamicScanName = "dynamic.scan";
		private const string dynamicBatchReadName = "dynamic.batch_read";
		private const string dynamicBatchWriteName = "dynamic.batch_write";
		private const string dynamicBatchDeleteName = "dynamic.batch_delete";
		private const string dynamicBatchUdfName = "dynamic.batch_udf";
		private const string dynamicTxnRollName = "dynamic.txn_roll";
		private const string dynamicTxnVerifyName = "dynamic.txn_verify";
		private const string dynamicMetricsName = "dynamic.metrics";
		private const string readModeAP = "read_mode_ap";
		private const string readModeSC = "read_mode_sc";
		private const string failOnFilteredOut = "fail_on_filtered_out";
		private const string replica = "replica";
		private const string sleepBetweenRetries = "sleep_between_retries";
		private const string socketTimeout = "socket_timeout";
		private const string timeoutDelay = "timeout_delay";
		private const string totalTimeout = "total_timeout";
		private const string maxRetries = "max_retries";
		private const string durableDelete = "durable_delete";
		private const string sendKey = "send_key";
		private const string maxConcurrentThreads = "max_concurrent_threads";
		private const string allowInline = "allow_inline";
		private const string allowInlineSSD = "allow_inline_ssd";
		private const string respondAllKeys = "respond_all_keys";

		#endregion

		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------

		private AerospikeClient client;
		
		private IConfigurationRoot configRoot;

		public IConfigurationData ConfigurationData { get; private set; }

		public int Interval { get; private set; }

		private readonly string filePath;

		private volatile bool modified = false;

		private readonly Dictionary<Version, IConfigurationData> supportedVersions = new() { { new Version(1, 0, 0), new ConfigurationDatav1_0_0() } };

		public static string GetConfigPath()
		{
			return Environment.GetEnvironmentVariable(CONFIG_PATH_ENV);
		}

		public static YamlConfigProvider CreateConfigProvider(string configEnvValue, AerospikeClient client)
		{
			try
			{
				return new YamlConfigProvider(configEnvValue, client);
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(e.Message);
				}
				return null;
			}
		}

		private YamlConfigProvider(string configEnvValue, AerospikeClient client)
		{
			this.client = client;
			ConfigurationData = null;
			Interval = IConfigProvider.DEFAULT_CONFIG_INTERVAL;

			if (Log.DebugEnabled())
			{
				Log.Debug("Supported YAML schema config versions: " + String.Join(", ", supportedVersions.Keys));
			}

			try
			{
				if (!configEnvValue.StartsWith(DEFAULT_CONFIG_URL_PREFIX))
				{
					configEnvValue = DEFAULT_CONFIG_URL_PREFIX + configEnvValue;
				}

				Uri envUri = new(configEnvValue);
				if (envUri.IsFile)
				{
					filePath = envUri.AbsolutePath;
					LoadConfig();
				}
				else
				{
					throw new AerospikeException("Invalid configuration path: " + configEnvValue);
				}
			}
			catch (Exception e)
			{
				throw new AerospikeException("Failed to parse " + filePath + ": " + e);
			}
		}

		private void Watch()
		{
			_ = this.configRoot.GetReloadToken().RegisterChangeCallback(_ => {
				modified = true;
				Watch();
			}, null);
		}

		/// <summary>
		/// Attempt to load a YAML configuration file from the yamlFilePath.
		/// </summary>
		/// <returns>true if a YAML config could be loaded and parsed.</returns>
		public bool LoadConfig()
		{
			if (configRoot == null)
			{
				try
				{
					configRoot = new ConfigurationBuilder()
					   .AddYamlFile(filePath, optional: false, reloadOnChange: true)
					   .Build();

					Version version = new(configRoot.GetSection("version").Value);

					if (!supportedVersions.TryGetValue(version, out IConfigurationData value))
					{
						Log.Warn("YAML config must contain a valid version field.");
						ConfigurationData = supportedVersions[new Version(1, 0, 0)]; // Default to the first supported version
					}
					else
					{
						ConfigurationData = value;
					}

					ProcessStaticConfig();
					ProcessDynamicConfig();
					LogConfigChanges(true);

					Watch();
					return true;
				}
				catch (Exception e)
				{
					throw new AerospikeException("Failed to load YAML configuration file: " + filePath + ". " + e.Message);
				}
			}

			if (modified) // Modified is set in the callback from the watch if file changes
			{
				ProcessDynamicConfig();
				LogConfigChanges(false);
				modified = false;
				return true;
			}
			return false;
		}

		private void ProcessStaticConfig()
		{
			configRoot.GetSection("static").Bind(ConfigurationData.staticConfig);

			if (ConfigurationData.staticConfig.client.config_interval.HasValue)
			{
				Interval = ConfigurationData.staticConfig.client.config_interval.Value;
			}
		}

		private void ProcessDynamicConfig()
		{
			var dynamicSection = configRoot.GetSection("dynamic");
			if (!dynamicSection.Exists())
			{
				// disable dynamic config
				ConfigurationData = null;
				Log.Info("Dynamic configuration has been disabled.");
			}
			else
			{
				dynamicSection.Bind(ConfigurationData.dynamicConfig);
				ConfigurationData.dynamicConfig.client.rack_ids = dynamicSection.GetSection("client:rack_ids").Get<int[]>();
				Log.Info("YAML config successfully loaded.");
			}
		}

		public void LogConfigChanges(bool init)
		{
			if (ConfigurationData == null)
			{
				// Dynamic config is disabled. return.
				return;
			}

			var clientPolicy = client.GetClientPolicy();

			if (init)
			{
				// static
				// client policy
				var staticClient = ConfigurationData.staticConfig.client;
				IConfigProvider.LogIntChange(staticClient.config_interval, IConfigProvider.DEFAULT_CONFIG_INTERVAL,
					staticClientName, "config_interval");

				IConfigProvider.LogIntChange(staticClient.max_connections_per_node, clientPolicy.maxConnsPerNode,
					staticClientName, "max_connections_per_node");
				IConfigProvider.LogIntChange(staticClient.min_connections_per_node, clientPolicy.minConnsPerNode,
					staticClientName, "min_connections_per_node");

				if (clientPolicy is AsyncClientPolicy asyncClientPolicy)
				{
					IConfigProvider.LogIntChange(staticClient.async_max_connections_per_node, asyncClientPolicy.asyncMaxConnsPerNode,
						staticClientName, "async_max_connections_per_node");
					IConfigProvider.LogIntChange(staticClient.async_min_connections_per_node, asyncClientPolicy.asyncMinConnsPerNode,
						staticClientName, "async_min_connections_per_node");
				}
			}

			// dynamic
			// client policy
			var dynamicClient = ConfigurationData.dynamicConfig.client;
			IConfigProvider.LogStringChange(dynamicClient.app_id, clientPolicy.AppId, 
				dynamicClientName, "app_id");
			IConfigProvider.LogIntChange(dynamicClient.timeout, clientPolicy.timeout, 
				dynamicClientName, "timeout");
			IConfigProvider.LogIntChange(dynamicClient.error_rate_window, clientPolicy.errorRateWindow, 
				dynamicClientName, "error_rate_window");
			IConfigProvider.LogIntChange(dynamicClient.max_error_rate, clientPolicy.maxErrorRate, 
				dynamicClientName, "max_error_rate");
			IConfigProvider.LogBoolChange(dynamicClient.fail_if_not_connected, clientPolicy.failIfNotConnected, 
				dynamicClientName, "fail_if_not_connected");
			IConfigProvider.LogIntChange(dynamicClient.login_timeout, clientPolicy.loginTimeout, 
				dynamicClientName, "login_timeout");
			IConfigProvider.LogIntChange(dynamicClient.max_socket_idle, clientPolicy.maxSocketIdle, 
				dynamicClientName, "max_socket_idle");
			IConfigProvider.LogBoolChange(dynamicClient.rack_aware, clientPolicy.rackAware, 
				dynamicClientName, "rack_aware");
			IConfigProvider.LogRackIdsChange(dynamicClient.rack_ids, clientPolicy.rackIds, 
				dynamicClientName, "rack_ids");
			IConfigProvider.LogIntChange(dynamicClient.tend_interval, clientPolicy.tendInterval, 
				dynamicClientName, "tend_interval");
			IConfigProvider.LogBoolChange(dynamicClient.use_service_alternative, clientPolicy.useServicesAlternate, 
				dynamicClientName, "use_service_alternative");

			// read policy
			var readPolicy = client.mergedReadPolicyDefault.Clone();
			var dynamicRead = ConfigurationData.dynamicConfig.read;
			IConfigProvider.LogReadModeAPChange(dynamicRead.read_mode_ap, readPolicy.readModeAP, 
				dynamicReadName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicRead.read_mode_sc, readPolicy.readModeSC, 
				dynamicReadName, readModeSC);
			IConfigProvider.LogBoolChange(dynamicRead.fail_on_filtered_out, readPolicy.failOnFilteredOut, 
				dynamicReadName, failOnFilteredOut);
			IConfigProvider.LogReplicaChange(dynamicRead.replica, readPolicy.replica, 
				dynamicReadName, replica);
			IConfigProvider.LogIntChange(dynamicRead.sleep_between_retries, readPolicy.sleepBetweenRetries, 
				dynamicReadName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicRead.socket_timeout, readPolicy.socketTimeout, 
				dynamicReadName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicRead.timeout_delay, readPolicy.TimeoutDelay, 
				dynamicReadName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicRead.total_timeout, readPolicy.totalTimeout,
				dynamicReadName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicRead.max_retries, readPolicy.maxRetries, 
				dynamicReadName, maxRetries);

			// write policy
			var writePolicy = client.mergedWritePolicyDefault.Clone();
			var dynamicWrite = ConfigurationData.dynamicConfig.write;
			IConfigProvider.LogBoolChange(dynamicWrite.fail_on_filtered_out, writePolicy.failOnFilteredOut, 
				dynamicWriteName, failOnFilteredOut);
			IConfigProvider.LogReplicaChange(dynamicWrite.replica, writePolicy.replica, 
				dynamicWriteName, replica);
			IConfigProvider.LogBoolChange(dynamicWrite.send_key, writePolicy.sendKey, 
				dynamicWriteName, sendKey);
			IConfigProvider.LogIntChange(dynamicWrite.sleep_between_retries, writePolicy.sleepBetweenRetries, 
				dynamicWriteName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicWrite.socket_timeout, writePolicy.socketTimeout,
				dynamicWriteName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicWrite.timeout_delay, writePolicy.TimeoutDelay, 
				dynamicWriteName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicWrite.total_timeout, writePolicy.totalTimeout, 
				dynamicWriteName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicWrite.max_retries, writePolicy.maxRetries, 
				dynamicWriteName, maxRetries);
			IConfigProvider.LogBoolChange(dynamicWrite.durable_delete, writePolicy.durableDelete, 
				dynamicWriteName, durableDelete);

			// query policy
			var queryPolicy = client.mergedQueryPolicyDefault.Clone();
			var dynamicQuery = ConfigurationData.dynamicConfig.query;
			IConfigProvider.LogReadModeAPChange(dynamicQuery.read_mode_ap, queryPolicy.readModeAP, 
				dynamicQueryName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicQuery.read_mode_sc, queryPolicy.readModeSC, 
				dynamicQueryName, readModeSC);
			IConfigProvider.LogReplicaChange(dynamicQuery.replica, queryPolicy.replica, 
				dynamicQueryName, replica);
			IConfigProvider.LogIntChange(dynamicQuery.sleep_between_retries, queryPolicy.sleepBetweenRetries, 
				dynamicQueryName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicQuery.socket_timeout, queryPolicy.socketTimeout, 
				dynamicQueryName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicQuery.timeout_delay, queryPolicy.TimeoutDelay, 
				dynamicQueryName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicQuery.total_timeout, queryPolicy.totalTimeout, 
				dynamicQueryName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicQuery.max_retries, queryPolicy.maxRetries, 
				dynamicQueryName, maxRetries);
			IConfigProvider.LogBoolChange(dynamicQuery.include_bin_data, queryPolicy.includeBinData, 
				dynamicQueryName, "include_bin_data");
			IConfigProvider.LogIntChange(dynamicQuery.info_timeout, (int)queryPolicy.infoTimeout, 
				dynamicQueryName, "info_timeout");
			IConfigProvider.LogIntChange(dynamicQuery.record_queue_size, queryPolicy.recordQueueSize,
				dynamicQueryName, "record_queue_size");
			IConfigProvider.LogQueryDurationChange(dynamicQuery.expected_duration, queryPolicy.expectedDuration, 
				dynamicQueryName, "expected_duration");

			// scan policy
			var scanPolicy = client.mergedScanPolicyDefault.Clone();
			var dynamicScan = ConfigurationData.dynamicConfig.scan;
			IConfigProvider.LogReadModeAPChange(dynamicScan.read_mode_ap, scanPolicy.readModeAP, 
				dynamicScanName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicScan.read_mode_sc, scanPolicy.readModeSC, 
				dynamicScanName, readModeSC);
			IConfigProvider.LogReplicaChange(dynamicScan.replica, scanPolicy.replica, 
				dynamicScanName, replica);
			IConfigProvider.LogIntChange(dynamicScan.sleep_between_retries, scanPolicy.sleepBetweenRetries, 
				dynamicScanName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicScan.socket_timeout, scanPolicy.socketTimeout, 
				dynamicScanName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicScan.timeout_delay, scanPolicy.TimeoutDelay, 
				dynamicScanName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicScan.total_timeout, scanPolicy.totalTimeout, 
				dynamicScanName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicScan.max_retries, scanPolicy.maxRetries, 
				dynamicScanName, maxRetries);
			IConfigProvider.LogBoolChange(dynamicScan.concurrent_nodes, scanPolicy.concurrentNodes, 
				dynamicScanName, "concurrent_nodes");
			IConfigProvider.LogIntChange(dynamicScan.max_concurrent_nodes, scanPolicy.maxConcurrentNodes, 
				dynamicScanName, "max_concurrent_nodes");

			// batch read policy
			var batchReadPolicy = client.mergedBatchPolicyDefault.Clone();
			var dynamicBatchRead = ConfigurationData.dynamicConfig.batch_read;
			IConfigProvider.LogReadModeAPChange(dynamicBatchRead.read_mode_ap, batchReadPolicy.readModeAP, 
				dynamicBatchReadName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicBatchRead.read_mode_sc, batchReadPolicy.readModeSC,
				dynamicBatchReadName, readModeSC);
			IConfigProvider.LogReplicaChange(dynamicBatchRead.replica, batchReadPolicy.replica, 
				dynamicBatchReadName, replica);
			IConfigProvider.LogIntChange(dynamicBatchRead.sleep_between_retries, batchReadPolicy.sleepBetweenRetries, 
				dynamicBatchReadName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicBatchRead.socket_timeout, batchReadPolicy.socketTimeout, 
				dynamicBatchReadName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicBatchRead.timeout_delay, batchReadPolicy.TimeoutDelay, 
				dynamicBatchReadName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicBatchRead.total_timeout, batchReadPolicy.totalTimeout, 
				dynamicBatchReadName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicBatchRead.max_retries, batchReadPolicy.maxRetries, 
				dynamicBatchReadName, maxRetries);
			IConfigProvider.LogIntChange(dynamicBatchRead.max_concurrent_threads, batchReadPolicy.maxConcurrentThreads, 
				dynamicBatchReadName, maxConcurrentThreads);
			IConfigProvider.LogBoolChange(dynamicBatchRead.allow_inline, batchReadPolicy.allowInline, 
				dynamicBatchReadName, allowInline);
			IConfigProvider.LogBoolChange(dynamicBatchRead.allow_inline_ssd, batchReadPolicy.allowInlineSSD, 
				dynamicBatchReadName, allowInlineSSD);
			IConfigProvider.LogBoolChange(dynamicBatchRead.respond_all_keys, batchReadPolicy.respondAllKeys, 
				dynamicBatchReadName, respondAllKeys);

			// batch write policy
			var batchWritePolicy = client.mergedBatchWritePolicyDefault.Clone();
			var batchParentWritePolicy = client.mergedBatchParentPolicyWriteDefault.Clone();
			var dynamicBatchWrite = ConfigurationData.dynamicConfig.batch_write;
			
			IConfigProvider.LogBoolChange(dynamicBatchWrite.durable_delete, batchWritePolicy.durableDelete,
				dynamicBatchWriteName, durableDelete);
			IConfigProvider.LogBoolChange(dynamicBatchWrite.send_key, batchWritePolicy.sendKey,
				dynamicBatchWriteName, sendKey);
			IConfigProvider.LogBoolChange(dynamicBatchWrite.fail_on_filtered_out, batchParentWritePolicy.failOnFilteredOut,
				dynamicBatchWriteName, failOnFilteredOut);
			IConfigProvider.LogReplicaChange(dynamicBatchWrite.replica, batchParentWritePolicy.replica,
				dynamicBatchWriteName, replica);
			IConfigProvider.LogIntChange(dynamicBatchWrite.sleep_between_retries, batchParentWritePolicy.sleepBetweenRetries,
				dynamicBatchWriteName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicBatchWrite.socket_timeout, batchParentWritePolicy.socketTimeout,
				dynamicBatchWriteName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicBatchWrite.timeout_delay, batchParentWritePolicy.TimeoutDelay,
				dynamicBatchWriteName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicBatchWrite.total_timeout, batchParentWritePolicy.totalTimeout,
				dynamicBatchWriteName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicBatchWrite.max_retries, batchParentWritePolicy.maxRetries,
				dynamicBatchWriteName, maxRetries);
			IConfigProvider.LogIntChange(dynamicBatchWrite.max_concurrent_threads, batchParentWritePolicy.maxConcurrentThreads,
				dynamicBatchWriteName, maxConcurrentThreads);
			IConfigProvider.LogBoolChange(dynamicBatchWrite.allow_inline, batchParentWritePolicy.allowInline,
				dynamicBatchWriteName, allowInline);
			IConfigProvider.LogBoolChange(dynamicBatchWrite.allow_inline_ssd, batchParentWritePolicy.allowInlineSSD,
				dynamicBatchWriteName, allowInlineSSD);
			IConfigProvider.LogBoolChange(dynamicBatchWrite.respond_all_keys, batchParentWritePolicy.respondAllKeys,
				dynamicBatchWriteName, respondAllKeys);

			// batch delete policy
			var batchDeletePolicy = client.mergedBatchDeletePolicyDefault.Clone();
			var dynamicBatchDelete = ConfigurationData.dynamicConfig.batch_delete;
			IConfigProvider.LogBoolChange(dynamicBatchDelete.durable_delete, batchDeletePolicy.durableDelete, 
				dynamicBatchDeleteName, durableDelete);
			IConfigProvider.LogBoolChange(dynamicBatchDelete.send_key, batchDeletePolicy.sendKey,
				dynamicBatchDeleteName, sendKey);

			// batch udf policy
			var batchUdfPolicy = client.mergedBatchUDFPolicyDefault.Clone();
			var dynamicBatchUdf = ConfigurationData.dynamicConfig.batch_udf;
			IConfigProvider.LogBoolChange(dynamicBatchUdf.durable_delete, batchUdfPolicy.durableDelete, 
				dynamicBatchUdfName, durableDelete);
			IConfigProvider.LogBoolChange(dynamicBatchUdf.send_key, batchUdfPolicy.sendKey,
				dynamicBatchUdfName, sendKey);

			// txn roll policy
			var txnRollPolicy = client.mergedTxnRollPolicyDefault.Clone();
			var dynamicTxnRoll = ConfigurationData.dynamicConfig.txn_roll;
			IConfigProvider.LogReadModeAPChange(dynamicTxnRoll.read_mode_ap, txnRollPolicy.readModeAP, 
				dynamicTxnRollName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicTxnRoll.read_mode_sc, txnRollPolicy.readModeSC,
				dynamicTxnRollName, readModeSC);
			IConfigProvider.LogReplicaChange(dynamicTxnRoll.replica, txnRollPolicy.replica,
				dynamicTxnRollName, replica);
			IConfigProvider.LogIntChange(dynamicTxnRoll.sleep_between_retries, txnRollPolicy.sleepBetweenRetries,
				dynamicTxnRollName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicTxnRoll.socket_timeout, txnRollPolicy.socketTimeout,
				dynamicTxnRollName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicTxnRoll.timeout_delay, txnRollPolicy.TimeoutDelay,
				dynamicTxnRollName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicTxnRoll.total_timeout, txnRollPolicy.totalTimeout, 
				dynamicTxnRollName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicTxnRoll.max_retries, txnRollPolicy.maxRetries, 
				dynamicTxnRollName, maxRetries);
			IConfigProvider.LogIntChange(dynamicTxnRoll.max_concurrent_threads, txnRollPolicy.maxConcurrentThreads, 
				dynamicTxnRollName, maxConcurrentThreads);
			IConfigProvider.LogBoolChange(dynamicTxnRoll.allow_inline, txnRollPolicy.allowInline,
				dynamicTxnRollName, allowInline);
			IConfigProvider.LogBoolChange(dynamicTxnRoll.allow_inline_ssd, txnRollPolicy.allowInlineSSD,
				dynamicTxnRollName, allowInlineSSD);
			IConfigProvider.LogBoolChange(dynamicTxnRoll.respond_all_keys, txnRollPolicy.respondAllKeys,
				dynamicTxnRollName, respondAllKeys);

			// txn verify policy
			var txnVerifyPolicy = client.mergedTxnVerifyPolicyDefault.Clone();
			var dynamicTxnVerify = ConfigurationData.dynamicConfig.txn_verify;
			IConfigProvider.LogReadModeAPChange(dynamicTxnVerify.read_mode_ap, txnVerifyPolicy.readModeAP, 
				dynamicTxnVerifyName, readModeAP);
			IConfigProvider.LogReadModeSCChange(dynamicTxnVerify.read_mode_sc, txnVerifyPolicy.readModeSC,
				dynamicTxnVerifyName, readModeSC);
			IConfigProvider.LogReplicaChange(dynamicTxnVerify.replica, txnVerifyPolicy.replica,
				dynamicTxnVerifyName, replica);
			IConfigProvider.LogIntChange(dynamicTxnVerify.sleep_between_retries, txnVerifyPolicy.sleepBetweenRetries,
				dynamicTxnVerifyName, sleepBetweenRetries);
			IConfigProvider.LogIntChange(dynamicTxnVerify.socket_timeout, txnVerifyPolicy.socketTimeout,
				dynamicTxnVerifyName, socketTimeout);
			IConfigProvider.LogIntChange(dynamicTxnVerify.timeout_delay, txnVerifyPolicy.TimeoutDelay,
				dynamicTxnVerifyName, timeoutDelay);
			IConfigProvider.LogIntChange(dynamicTxnVerify.total_timeout, txnVerifyPolicy.totalTimeout,
				dynamicTxnVerifyName, totalTimeout);
			IConfigProvider.LogIntChange(dynamicTxnVerify.max_retries, txnVerifyPolicy.maxRetries,
				dynamicTxnVerifyName, maxRetries);
			IConfigProvider.LogIntChange(dynamicTxnVerify.max_concurrent_threads, txnVerifyPolicy.maxConcurrentThreads,
				dynamicTxnVerifyName, maxConcurrentThreads);
			IConfigProvider.LogBoolChange(dynamicTxnVerify.allow_inline, txnVerifyPolicy.allowInline,
				dynamicTxnVerifyName, allowInline);
			IConfigProvider.LogBoolChange(dynamicTxnVerify.allow_inline_ssd, txnVerifyPolicy.allowInlineSSD, 
				dynamicTxnVerifyName, allowInlineSSD);
			IConfigProvider.LogBoolChange(dynamicTxnVerify.respond_all_keys, txnVerifyPolicy.respondAllKeys, 
				dynamicTxnVerifyName, respondAllKeys);

			// metrics policy
			var dynamicMetrics = ConfigurationData.dynamicConfig.metrics;
			var metricsPolicy = new MetricsPolicy();

			if (client.cluster != null)
			{
				lock (client.cluster.metricsLock)
				{
					if (client.cluster.MetricsPolicy != null)
					{
						metricsPolicy = client.cluster.MetricsPolicy;
					}

					IConfigProvider.LogBoolChange(dynamicMetrics.enable, client.cluster.MetricsEnabled,
						dynamicMetricsName, "enabled");
					IConfigProvider.LogIntChange(dynamicMetrics.latency_shift, metricsPolicy.LatencyShift,
						dynamicMetricsName, "latency_shift");
					IConfigProvider.LogIntChange(dynamicMetrics.latency_columns, metricsPolicy.LatencyColumns,
						dynamicMetricsName, "latency_columns");
					IConfigProvider.LogStringDictionaryChange(dynamicMetrics.labels, metricsPolicy.labels,
						dynamicMetricsName, "labels");
				}
			}
			else
			{
				// Client hasn't initialized the cluster yet. Use disabled as the default for metrics.
				IConfigProvider.LogBoolChange(dynamicMetrics.enable, false,
					dynamicMetricsName, "enabled");
				IConfigProvider.LogIntChange(dynamicMetrics.latency_shift, metricsPolicy.LatencyShift,
					dynamicMetricsName, "latency_shift");
				IConfigProvider.LogIntChange(dynamicMetrics.latency_columns, metricsPolicy.LatencyColumns,
					dynamicMetricsName, "latency_columns");
				IConfigProvider.LogStringDictionaryChange(dynamicMetrics.labels, metricsPolicy.labels,
					dynamicMetricsName, "labels");
			}
			
		}
	}
}
