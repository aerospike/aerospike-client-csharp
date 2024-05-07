/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	/// Container object for policy attributes used in query operations.
	/// </summary>
	public class QueryPolicy : Policy
	{
		/// <summary>
		/// Expected query duration. The server treats the query in different ways depending on the expected duration.
		/// This field is ignored for aggregation queries, background queries and server versions &lt; 6.0.
		/// <para>
		/// Default: <see cref="QueryDuration.LONG"/>
		/// </para>
		/// </summary>
		public QueryDuration expectedDuration;

		/// <summary>
		/// Approximate number of records to return to client. This number is divided by the
		/// number of nodes involved in the query.  The actual number of records returned
		/// may be less than maxRecords if node record counts are small and unbalanced across
		/// nodes.
		/// <para>
		/// maxRecords is only supported when query filter is null.  maxRecords
		/// exists here because query methods will convert into a scan when the query
		/// filter is null.  maxRecords is ignored when the query contains a filter.
		/// </para>
		/// <para>
		/// Default: 0 (do not limit record count)
		/// </para>
		/// </summary>
		[Obsolete("Use 'Statement.MaxRecords' instead.")]
		public long maxRecords;

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
		/// Terminate query if cluster is in migration state. If the server supports partition
		/// queries or the query filter is null (scan), this field is ignored.
		/// <para>Default: false</para>
		/// </summary>
		public bool failOnClusterChange;

		/// <summary>
		/// This field is deprecated and will eventually be removed. Use <see cref="expectedDuration"/> 
		/// instead.
		/// <para>
		/// For backwards compatibility: If shortQuery is true, the query is treated as a short
		/// query and <see cref="expectedDuration"/> is ignored. If shortQuery is false, 
		/// <see cref="expectedDuration"/> is used and defaults to <see cref="QueryDuration.LONG"/>.
		/// </para>
		/// Is query expected to return less than 100 records per node.
		/// If true, the server will optimize the query for a small record set.
		/// This field is ignored for aggregation queries, background queries
		/// and server versions &lt; 6.0.
		/// <para>Default: false</para>
		/// </summary>
		[Obsolete("Use 'expectedDuration' instead.")]
		public bool shortQuery;
		public uint infoTimeout;

		/// <summary>
		/// Copy query policy from another query policy.
		/// </summary>
		public QueryPolicy(QueryPolicy other) : base(other)
		{
			this.expectedDuration = other.expectedDuration;
			this.maxRecords = other.maxRecords;
			this.maxConcurrentNodes = other.maxConcurrentNodes;
			this.recordQueueSize = other.recordQueueSize;
			this.includeBinData = other.includeBinData;
			this.failOnClusterChange = other.failOnClusterChange;
			this.shortQuery = other.shortQuery;
		}

		/// <summary>
		/// Copy query policy from another policy.
		/// </summary>
		public QueryPolicy(Policy other)
			: base(other)
		{
		}

		/// <summary>
		/// Default constructor. Disable totalTimeout and set maxRetries.
		/// <para>
		/// The latest servers support retries on individual data partitions.
		/// This feature is useful when a cluster is migrating and partition(s)
		/// are missed or incomplete on the first query attempt.
		/// </para>
		/// <para>
		/// If the first query attempt misses 2 of 4096 partitions, then only
		/// those 2 partitions are retried in the next query attempt from the
		/// last key digest received for each respective partition. A higher
		/// default maxRetries is used because it's wasteful to invalidate
		/// all query results because a single partition was missed.
		/// </para>
		/// </summary>
		public QueryPolicy() : base()
		{
			base.totalTimeout = 0;
			base.maxRetries = 5;
		}
	}
}
#pragma warning restore 0618
