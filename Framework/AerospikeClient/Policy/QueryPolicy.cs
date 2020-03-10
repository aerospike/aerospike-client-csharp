/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	/// Container object for policy attributes used in query operations.
	/// </summary>
	public class QueryPolicy : Policy
	{
		/// <summary>
		/// Maximum number of concurrent requests to server nodes at any point in time.
		/// If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then queries 
		/// will be made to 8 nodes in parallel.  When a query completes, a new query will 
		/// be issued until all 16 nodes have been queried.
		/// <para>Default: 0 (issue requests to all server nodes in parallel)</para>
		/// </summary>
		public int maxConcurrentNodes;

		/// <summary>
		/// Number of records to place in queue before blocking.
		/// Records received from multiple server nodes will be placed in a queue.
		/// A separate thread consumes these records in parallel.
		/// If the queue is full, the producer threads will block until records are consumed.
		/// <para>Default: 5000</para>
		/// </summary>
		public int recordQueueSize = 5000;

		/// <summary>
		/// Should bin data be retrieved. If false, only record digests (and user keys
		/// if stored on the server) are retrieved.
		/// <para>Default: true</para>
		/// </summary>
		public bool includeBinData = true;

		/// <summary>
		/// Terminate query if cluster is in migration state.
		/// Only used for server versions &lt; 4.9.
		/// <para>Default: false</para>
		/// </summary>
		public bool failOnClusterChange;

		/// <summary>
		/// Copy query policy from another query policy.
		/// </summary>
		public QueryPolicy(QueryPolicy other) : base(other)
		{
			this.maxConcurrentNodes = other.maxConcurrentNodes;
			this.recordQueueSize = other.recordQueueSize;
			this.includeBinData = other.includeBinData;
			this.failOnClusterChange = other.failOnClusterChange;
		}

		/// <summary>
		/// Default constructor.
		/// <para>
		/// Set maxRetries for non-aggregation queries with a null filter on
		/// server versions >= 4.9. All other queries are not retried.
		/// </para>
		/// <para>
		/// The latest servers support retries on individual data partitions.
		/// This feature is useful when a cluster is migrating and partition(s)
		/// are missed or incomplete on the first query (with null filter) attempt.
		/// </para>
		/// <para>
		/// If the first query attempt misses 2 of 4096 partitions, then only
		/// those 2 partitions are retried in the next query attempt from the
		/// last key digest received for each respective partition. A higher
		/// default maxRetries is used because it's wasteful to invalidate
		/// all query results because a single partition was missed.
		/// </para>
		/// </summary>
		public QueryPolicy()
		{
			base.maxRetries = 5;
		}
	}
}
