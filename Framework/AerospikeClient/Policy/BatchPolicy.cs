/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
namespace Aerospike.Client
{
	/// <summary>
	/// Configuration variables for multi-record get and exist requests.
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
		///	Asynchronous batch requests ignore this field and always issue all node requests in parallel.
		/// </summary>		
		public int maxConcurrentThreads = 1;

		/// <summary>
		/// Allow batch to be processed immediately in the server's receiving thread when the server
		/// deems it to be appropriate.  If false, the batch will always be processed in separate
		/// transaction threads.  This field is only relevant for the new batch index protocol.
		/// <para>
		/// For batch exists or batch reads of smaller sized records (less than 1K per record),
		/// inline processing will be significantly faster on "in memory" namespaces.  The server
		/// disables inline processing on disk based namespaces regardless of this policy field.
		/// </para>
		/// <para>
		/// Inline processing can introduce the possibility of unfairness because the server
		/// can process the entire batch before moving onto the next command.
		/// Default: true
		/// </para>
		/// </summary>
		public bool allowInline = true;
	
		/// <summary>
		/// Allow read operations to use replicated data partitions instead of master
		/// partition. By default, both read and write operations are directed to the
		/// master partition.
		/// <para>
		/// This variable is currently only used in batch read/exists operations. For 
		/// batch, this variable should only be set to true when the replication factor
		/// is greater than or equal to the number of nodes in the cluster.
		/// </para>
		/// </summary>
		public bool allowProleReads;

		/// <summary>
		/// Send set name field to server for every key in the batch for batch index protocol. 
		/// This is only necessary when authentication is enabled and security roles are defined
		/// on a per set basis.
		/// Default: false
		/// </summary>
		public bool sendSetName;

		/// <summary>
		/// Copy batch policy from another batch policy.
		/// </summary>
		public BatchPolicy(BatchPolicy other)
			: base(other)
		{
			this.maxConcurrentThreads = other.maxConcurrentThreads;
			this.allowInline = other.allowInline;
			this.allowProleReads = other.allowProleReads;
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
		/// Default constructor.
		/// </summary>
		public BatchPolicy()
		{
		}
	}
}
