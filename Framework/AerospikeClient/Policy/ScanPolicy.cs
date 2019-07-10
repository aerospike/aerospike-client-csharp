/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	/// Container object for optional parameters used in scan operations.
	/// </summary>
	public sealed class ScanPolicy : Policy
	{
		/// <summary>
		/// Percent of data to scan.  Valid integer range is 1 to 100.
		/// <para>Default: 100</para>
		/// </summary>
		public int scanPercent = 100;

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
		/// Terminate scan if cluster is in migration state.
		/// <para>Default: false</para>
		/// </summary>
		public bool failOnClusterChange;

		/// <summary>
		/// Copy scan policy from another scan policy.
		/// </summary>
		public ScanPolicy(ScanPolicy other) : base(other)
		{
			this.scanPercent = other.scanPercent;
			this.maxConcurrentNodes = other.maxConcurrentNodes;
			this.concurrentNodes = other.concurrentNodes;
			this.includeBinData = other.includeBinData;
			this.failOnClusterChange = other.failOnClusterChange;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public ScanPolicy()
		{
			// Scans should not retry.
			base.maxRetries = 0;
		}
	}
}
