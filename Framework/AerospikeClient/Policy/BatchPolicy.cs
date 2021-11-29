/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System;

#pragma warning disable 0618

namespace Aerospike.Client
{
	/// <summary>
	/// Batch parent policy.
	/// </summary>
	public sealed class BatchPolicy : Policy
	{
		/// <summary>
		/// Maximum number of concurrent synchronous batch request threads to server nodes at any point in time.
		/// If there are 16 node/namespace combinations requested and maxConcurrentThreads is 8, 
		/// then batch requests will be made for 8 node/namespace combinations in parallel threads.
		/// When a request completes, a new request will be issued until all 16 threads are complete.
		/// <para>
		/// Values:
		/// <list type="bullet">
		/// <item>
		/// 1: Issue batch requests sequentially.  This mode has a performance advantage for small
		/// to medium sized batch sizes because requests can be issued in the main transaction thread.
		/// This is the default.
		/// </item>
		/// <item>
		/// 0: Issue all batch requests in parallel threads.  This mode has a performance
		/// advantage for extremely large batch sizes because each node can process the request
		/// immediately.  The downside is extra threads will need to be created (or taken from
		/// a thread pool).
		/// </item>
		/// <item>
		/// > 0: Issue up to maxConcurrentThreads batch requests in parallel threads.  When a request
		/// completes, a new request will be issued until all threads are complete.  This mode
		/// prevents too many parallel threads being created for large cluster implementations.
		/// The downside is extra threads will still need to be created (or taken from a thread pool).
		/// </item>
		/// </list>
		/// </para>
		/// <para>
		///	Asynchronous batch requests ignore this field and always issue all node requests in parallel.
		///	</para>
		/// <para>Default: 1</para>
		/// </summary>		
		public int maxConcurrentThreads = 1;

		/// <summary>
		/// <see cref="InlineFlags"/> indicate when the server should inline batch
		/// sub-transactions. Inlined transactions are processed in the server's receiving thread.
		/// Non-inlined transactions are processed in separate server transaction threads.
		/// <para>
		/// Inline processing can introduce the possibility of unfairness because the server
		/// can process the entire batch before moving onto the next command.
		/// </para>
		/// <para>
		/// Default: <see cref="InlineFlags.INLINE_IN_MEMORY"/>
		/// </para>
		/// </summary>
		/// <example>
		/// <code>
		/// // Inline all example
		/// BatchPolicy bp = new BatchPolicy();
		/// bp.inline = InlineFlags.INLINE_IN_MEMORY | InlineFlags.INLINE_ON_DEVICE;
		/// </code>
		/// </example>
		public InlineFlags inline = InlineFlags.INLINE_IN_MEMORY;
	
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
		/// Should all batch keys be attempted regardless of errors.
		/// <para>
		/// If true, every batch key is attempted regardless of previous key specific errors.
		/// Node specific errors such as timeouts stop keys to that node, but keys directed at
		/// other nodes will continue to be processed.
		/// </para>
		/// <para>
		/// If false, most key and node specific errors stop the batch. The exceptions are
		/// <see cref="Aerospike.Client.ResultCode.KEY_NOT_FOUND_ERROR"/> and
		/// <see cref="Aerospike.Client.ResultCode.FILTERED_OUT"/> which never stop the batch.
		/// </para>
		/// <para>
		/// This field is used on both the client and server. The client handles node specific
		/// errors and the server handles key specific errors. Server versions &lt; 5.8
		/// do not support <see cref="Aerospike.Client.BatchPolicy.respondAllKeys"/> and treat this value as false.
		/// </para>
		/// <para>Default: true</para>
		/// </summary>
		public bool respondAllKeys = true;

		/// <summary>
		/// Copy batch policy from another batch policy.
		/// </summary>
		public BatchPolicy(BatchPolicy other)
			: base(other)
		{
			this.maxConcurrentThreads = other.maxConcurrentThreads;
			this.inline = other.inline;
			this.allowProleReads = other.allowProleReads;
			this.respondAllKeys = other.respondAllKeys;
		}

		/// <summary>
		/// Copy batch policy from another policy.
		/// </summary>
		public BatchPolicy(Policy other)
			: base(other)
		{
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
	}
}

#pragma warning restore 0618
