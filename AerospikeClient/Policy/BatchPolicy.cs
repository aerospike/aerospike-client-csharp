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
#pragma warning disable 0618

namespace Aerospike.Client
{
	/// <summary>
	/// Batch parent policy.
	/// </summary>
	public class BatchPolicy : Policy
	{
		/// <summary>
		/// Maximum number of concurrent synchronous batch node request threads to server nodes.
		/// Asynchronous batch requests ignore this field and always issue all node requests in parallel.
		/// <para>
		/// The batch is split into requests for each node according to the node assignment of each
		/// batch key. The number of batch node requests is always less or equal to the cluster size.
		/// </para>
		/// <para>
		/// If there are 16 batch node requests and maxConcurrentThreads is 8, then batch requests
		/// will be made for 8 nodes in parallel threads. When a request completes, a new request will
		/// be issued until all 16 requests are complete. If there are 4 batch node requests and
		/// maxConcurrentThreads is 8, then only 4 batch requests will be made for 4 nodes in parallel
		/// threads.
		/// </para>
		/// <para>
		/// Values:
		/// <ul>
		/// <li>
		/// 1 (default): Issue batch node requests sequentially. This mode has a performance advantage
		/// for small batch sizes because requests can be issued in the main command thread without
		/// using a thread pool. This mode is not optimal for batch requests spread out over many nodes
		/// in a large cluster.
		/// </li>
		/// <li>
		/// 0: Issue all batch node requests in parallel threads. This mode has a performance advantage
		/// for large batch sizes because each node can process the request immediately. The downside is
		/// extra threads will need to be created (or taken from a thread pool). In extreme cases, the
		/// operating system's thread capacity could be exhausted.
		/// </li>
		/// <li>
		/// > 0: Issue up to maxConcurrentThreads batch node requests in parallel threads. When a request
		/// completes, a new request will be issued until all requests are complete. This mode prevents
		/// too many parallel threads being created for large clusters. The downside is extra threads
		/// will still need to be created (or taken from a thread pool). A typical value is the number
		/// of cpu cores available on the client machine.
		/// </li>
		/// </ul>
		/// </para>
		/// <para>Default: 1</para>
		/// </summary>
		public int maxConcurrentThreads = 1;

		/// <summary>
		/// Allow batch to be processed immediately in the server's receiving thread for in-memory
		/// namespaces. If false, the batch will always be processed in separate service threads.
		/// <para>
		/// For batch commands with smaller sized records (&lt;= 1K per record), inline
		/// processing will be significantly faster on in-memory namespaces.
		/// </para>
		/// <para>
		/// Inline processing can introduce the possibility of unfairness because the server
		/// can process the entire batch before moving onto the next command.
		/// </para>
		/// <para>
		/// Default: true
		/// </para>
		/// </summary>
		public bool allowInline = true;

		/// <summary>
		/// Allow batch to be processed immediately in the server's receiving thread for SSD
		/// namespaces. If false, the batch will always be processed in separate service threads.
		/// Server versions &lt; 6.0 ignore this field.
		/// <para>
		/// Inline processing can introduce the possibility of unfairness because the server
		/// can process the entire batch before moving onto the next command.
		/// </para>
		/// <para>
		/// Default: false
		/// </para>
		/// </summary>
		public bool allowInlineSSD = false;

		/// <summary>
		/// Allow read operations to use replicated data partitions instead of master
		/// partition. By default, both read and write operations are directed to the
		/// master partition.
		/// <para>
		/// This variable is currently only used in batch read/exists operations. For 
		/// batch, this variable should only be set to true when the replication factor
		/// is greater than or equal to the number of nodes in the cluster.
		/// </para>
		/// <para>Default: false</para>
		/// </summary>
		public bool allowProleReads;

		/// <summary>
		/// Should all batch keys be attempted regardless of errors. This field is used on both
		/// the client and server. The client handles node specific errors and the server handles
		/// key specific errors.
		/// <para>
		/// If true, every batch key is attempted regardless of previous key specific errors.
		/// Node specific errors such as timeouts stop keys to that node, but keys directed at
		/// other nodes will continue to be processed.
		/// </para>
		/// <para>
		/// If false, the server will stop the batch to its node on most key specific errors.
		/// The exceptions are <see cref="Aerospike.Client.ResultCode.KEY_NOT_FOUND_ERROR"/> and
		/// <see cref="Aerospike.Client.ResultCode.FILTERED_OUT"/> which never stop the batch.
		/// The client will stop the entire batch on node specific errors for sync commands
		/// that are run in sequence (maxConcurrentThreads == 1). The client will not stop
		/// the entire batch for async commands or sync commands run in parallel.
		/// </para>
		/// <para>
		/// Server versions &lt; 6.0 do not support this field and treat this value as false
		/// for key specific errors.
		/// </para>
		/// <para>Default: true</para>
		/// </summary>
		public bool respondAllKeys = true;

		/// <summary>
		/// This field is deprecated and will eventually be removed.
		/// The set name is now always sent for every distinct namespace/set in the batch.
		/// </summary>
		[Obsolete("Deprecated. The set name is now always sent.")]
		public bool sendSetName;

		/// <summary>
		/// Copy batch policy from another batch policy.
		/// </summary>
		public BatchPolicy(BatchPolicy other)
			: base(other)
		{
			this.maxConcurrentThreads = other.maxConcurrentThreads;
			this.allowInline = other.allowInline;
			this.allowInlineSSD = other.allowInlineSSD;
			this.allowProleReads = other.allowProleReads;
			this.respondAllKeys = other.respondAllKeys;
			this.sendSetName = other.sendSetName;
		}

		/// <summary>
		/// Copy batch policy from another policy.
		/// </summary>
		public BatchPolicy(Policy other)
			: base(other)
		{
		}

		/// <summary>
		/// Copy batch policy from another policy and override according to the AerospikeConfigProvider.
		/// </summary>
		public BatchPolicy(BatchPolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var batch_read = configProvider.ConfigurationData.dynamicConfig.batch_read;
			if (batch_read == null)
			{
				return;
			}

			if (batch_read.read_mode_ap.HasValue)
			{
				this.readModeAP = batch_read.read_mode_ap.Value;
			}
			if (batch_read.read_mode_sc.HasValue)
			{
				this.readModeSC = batch_read.read_mode_sc.Value;
			}
			if (batch_read.replica.HasValue)
			{
				this.replica = batch_read.replica.Value;
			}
			if (batch_read.sleep_between_retries.HasValue)
			{
				this.sleepBetweenRetries = batch_read.sleep_between_retries.Value;
			}
			if (batch_read.socket_timeout.HasValue)
			{
				this.socketTimeout = batch_read.socket_timeout.Value;
			}
			if (batch_read.timeout_delay.HasValue)
			{
				this.TimeoutDelay = batch_read.timeout_delay.Value;
			}
			if (batch_read.total_timeout.HasValue)
			{
				this.totalTimeout = batch_read.total_timeout.Value;
			}
			if (batch_read.max_retries.HasValue)
			{
				this.maxRetries = batch_read.max_retries.Value;
			}
			if (batch_read.max_concurrent_threads.HasValue)
			{
				this.maxConcurrentThreads = batch_read.max_concurrent_threads.Value;
			}
			if (batch_read.allow_inline.HasValue)
			{
				this.allowInline = batch_read.allow_inline.Value;
			}
			if (batch_read.allow_inline_ssd.HasValue)
			{
				this.allowInlineSSD = batch_read.allow_inline_ssd.Value;
			}
			if (batch_read.respond_all_keys.HasValue)
			{
				this.respondAllKeys = batch_read.respond_all_keys.Value;
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public BatchPolicy()
		{
		}

		/// <summary>
		/// Default batch read policy.
		/// </summary>
		public static BatchPolicy ReadDefault()
		{
			return new BatchPolicy();
		}

		/// <summary>
		/// Default batch write policy.
		/// </summary>
		public static BatchPolicy WriteDefault()
		{
			BatchPolicy policy = new BatchPolicy();
			policy.maxRetries = 0;
			return policy;
		}

		/// <summary>
		/// Creates a deep copy of this batch policy.
		/// </summary>
		/// <returns></returns>
		public new BatchPolicy Clone()
		{
			return new BatchPolicy(this);
		}

		public void GraftBatchWriteConfig(IConfigProvider configProvider)
		{
			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var batch_write = configProvider.ConfigurationData.dynamicConfig.batch_write;
			if (batch_write == null)
			{
				return;
			}

			if (batch_write.fail_on_filtered_out.HasValue)
			{
				this.failOnFilteredOut = batch_write.fail_on_filtered_out.Value;
			}
			if (batch_write.replica.HasValue)
			{
				this.replica = batch_write.replica.Value;
			}
			if (batch_write.sleep_between_retries.HasValue)
			{
				this.sleepBetweenRetries = batch_write.sleep_between_retries.Value;
			}
			if (batch_write.socket_timeout.HasValue)
			{
				this.socketTimeout = batch_write.socket_timeout.Value;
			}
			if (batch_write.timeout_delay.HasValue)
			{
				this.TimeoutDelay = batch_write.timeout_delay.Value;
			}
			if (batch_write.total_timeout.HasValue)
			{
				this.totalTimeout = batch_write.total_timeout.Value;
			}
			if (batch_write.max_retries.HasValue)
			{
				this.maxRetries = batch_write.max_retries.Value;
			}
			if (batch_write.send_key.HasValue)
			{
				this.sendKey = batch_write.send_key.Value;
			}
			if (batch_write.max_concurrent_threads.HasValue)
			{
				this.maxConcurrentThreads = batch_write.max_concurrent_threads.Value;
			}
			if (batch_write.allow_inline.HasValue)
			{
				this.allowInline = batch_write.allow_inline.Value;
			}
			if (batch_write.allow_inline_ssd.HasValue)
			{
				this.allowInlineSSD = batch_write.allow_inline_ssd.Value;
			}
			if (batch_write.respond_all_keys.HasValue)
			{
				this.respondAllKeys = batch_write.respond_all_keys.Value;
			}
		}
	}
}

#pragma warning restore 0618
