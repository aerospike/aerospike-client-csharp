/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
		/// Default is 100.
		/// </summary>
		public int scanPercent = 100;

		/// <summary>
		/// Maximum number of concurrent requests to server nodes at any point in time.
		/// If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then scan requests
		/// will be made to 8 nodes in parallel.  When a scan completes, a new scan request will 
		/// be issued until all 16 nodes have been scanned.
		/// <para>
		/// This field is only relevant when concurrentNodes is true.
		/// Default (0) is to issue requests to all server nodes in parallel.
		/// </para>
		/// </summary>
		public int maxConcurrentNodes;
	
		/// <summary>
		/// Issue scan requests in parallel or serially. 
		/// </summary>
		public bool concurrentNodes = true;

		/// <summary>
		/// Indicates if bin data is retrieved. If false, only record digests are retrieved.
		/// </summary>
		public bool includeBinData = true;

		/// <summary>
		/// Include large data type bin values in addition to large data type bin names.
		/// If false, LDT bin names will be returned, but LDT bin values will be empty.
		/// If true,  LDT bin names and the entire LDT bin values will be returned.
		/// Warning: LDT values may consume huge of amounts of memory depending on LDT size.
		/// Default: false
		/// </summary>
		public bool includeLDT = false;
	
		/// <summary>
		/// Terminate scan if cluster in fluctuating state.
		/// </summary>
		public bool failOnClusterChange;

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
