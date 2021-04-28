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
	/// Container object for optional parameters used in scan operations.
	/// </summary>
	public sealed class ScanPolicy : Policy
	{
		/// <summary>
		/// Approximate number of records to return to client. This number is divided by the
		/// number of nodes involved in the scan.  The actual number of records returned
		/// may be less than maxRecords if node record counts are small and unbalanced across
		/// nodes.
		/// <para>
		/// Default: 0 (do not limit record count)
		/// </para>
		/// </summary>
		public long maxRecords;
	
		/// <summary>
		/// Limit returned records per second (rps) rate for each server.
		/// Do not apply rps limit if recordsPerSecond is zero.
		/// <para>
		/// Default: 0
		/// </para>
		/// </summary>
		public int recordsPerSecond;

		/// <summary>
		/// Maximum number of concurrent requests to server nodes at any point in time.
		/// If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then scan requests
		/// will be made to 8 nodes in parallel.  When a scan completes, a new scan request will 
		/// be issued until all 16 nodes have been scanned.
		/// <para>
		/// This field is only relevant when concurrentNodes is true.
		/// </para>
		/// <para>Default: 0 (issue requests to all server nodes in parallel)</para>
		/// </summary>
		public int maxConcurrentNodes;

		/// <summary>
		/// Should scan requests be issued in parallel. 
		/// <para>Default: true</para>
		/// </summary>
		public bool concurrentNodes = true;

		/// <summary>
		/// Should bin data be retrieved. If false, only record digests (and user keys
		/// if stored on the server) are retrieved.
		/// <para>Default: true</para>
		/// </summary>
		public bool includeBinData = true;

		/// <summary>
		/// Copy scan policy from another scan policy.
		/// </summary>
		public ScanPolicy(ScanPolicy other) : base(other)
		{
			this.maxRecords = other.maxRecords;
			this.recordsPerSecond = other.recordsPerSecond;
			this.maxConcurrentNodes = other.maxConcurrentNodes;
			this.concurrentNodes = other.concurrentNodes;
			this.includeBinData = other.includeBinData;
		}

		/// <summary>
		/// Default constructor.
		/// <para>
		/// Set maxRetries for scans. The latest servers support retries on
		/// individual data partitions. This feature is useful when a cluster
		/// is migrating and partition(s) are missed or incomplete on the first
		/// scan attempt.
		/// </para>
		/// <para>
		/// If the first scan attempt misses 2 of 4096 partitions, then only
		/// those 2 partitions are retried in the next scan attempt from the
		/// last key digest received for each respective partition.  A higher
		/// default maxRetries is used because it's wasteful to invalidate
		/// all scan results because a single partition was missed.
		/// </para>
		/// </summary>
		public ScanPolicy()
		{
			base.maxRetries = 5;
		}

		/// <summary>
		/// Verify policies fields are within range.
		/// </summary>
		public void Validate()
		{
			if (maxRecords < 0)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid maxRecords: " + maxRecords);
			}
		}
	}
}
#pragma warning restore 0618
