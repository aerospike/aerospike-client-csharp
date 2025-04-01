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
	/// Container object for optional parameters used in scan operations.
	/// <para>
	/// Inherited Policy fields <see cref="Policy.Txn"/> and 
	/// <see cref="Policy.failOnFilteredOut"/> are ignored.
	/// </para>
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
		/// Number of records to place in queue before blocking.
		/// Records received from multiple server nodes will be placed in a queue.
		/// A separate thread consumes these records in parallel.
		/// If the queue is full, the producer threads will block until records are consumed.
		/// <para>Default: 5000</para>
		/// </summary>
		public int recordQueueSize = 5000;

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
			this.recordQueueSize = other.recordQueueSize;
		}

		/// <summary>
		/// Copy scan policy from another policy.
		/// </summary>
		public ScanPolicy(Policy other)
			: base(other)
		{
		}

		/// <summary>
		/// Default constructor. Disable totalTimeout and set maxRetries.
		/// <para>
		/// The latest servers support retries on individual data partitions.
		/// This feature is useful when a cluster is migrating and partition(s)
		/// are missed or incomplete on the first scan attempt.
		/// </para>
		/// <para>
		/// If the first scan attempt misses 2 of 4096 partitions, then only
		/// those 2 partitions are retried in the next scan attempt from the
		/// last key digest received for each respective partition. A higher
		/// default maxRetries is used because it's wasteful to invalidate
		/// all scan results because a single partition was missed.
		/// </para>
		/// </summary>
		public ScanPolicy() : base()
		{
			base.totalTimeout = 0;
			base.maxRetries = 5;
		}

		/// <summary>
		/// Creates a deep copy of this scan policy.
		/// </summary>
		/// <returns></returns>
		public new ScanPolicy Clone()
		{
			return new ScanPolicy(this);
		}

        public override void ApplyConfigOverrides(IAerospikeConfigProvider config)
        {
            var scan = config.DynamicProperties.scan;

            if (scan.read_mode_ap.HasValue)
            {
                this.readModeAP = scan.read_mode_ap.Value;
            }
            if (scan.read_mode_sc.HasValue)
            {
                this.readModeSC = scan.read_mode_sc.Value;
            }
            if (scan.replica.HasValue)
            {
                this.replica = scan.replica.Value;
            }
            if (scan.sleep_between_retries.HasValue)
            {
                this.sleepBetweenRetries = scan.sleep_between_retries.Value;
            }
            if (scan.socket_timeout.HasValue)
            {
                this.socketTimeout = scan.socket_timeout.Value;
            }
            if (scan.timeout_delay.HasValue)
            {
                this.TimeoutDelay = scan.timeout_delay.Value;
            }
            if (scan.concurrent_nodes.HasValue)
            {
                this.concurrentNodes = scan.concurrent_nodes.Value;
            }
			if (scan.max_concurrent_nodes.HasValue)
			{
				this.maxConcurrentNodes = scan.max_concurrent_nodes.Value;
            }

            Log.Debug("ScanPolicy has been aligned with config properties.");
        }
    }
}
#pragma warning restore 0618
