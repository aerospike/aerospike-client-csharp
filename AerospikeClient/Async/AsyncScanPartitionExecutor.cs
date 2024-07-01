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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncScanPartitionExecutor : AsyncMultiExecutor
	{
		private readonly ScanPolicy policy;
		private readonly RecordSequenceListener listener;
		private readonly string ns;
		private readonly string setName;
		private readonly string[] binNames;
		private readonly PartitionTracker tracker;

		public AsyncScanPartitionExecutor
		(
			AsyncCluster cluster,
			ScanPolicy policy,
			RecordSequenceListener listener,
			string ns,
			string setName,
			string[] binNames,
			PartitionTracker tracker
		) : base(cluster)
		{
			this.policy = policy;
			this.listener = listener;
			this.ns = ns;
			this.setName = setName;
			this.binNames = binNames;
			this.tracker = tracker;

			cluster.AddTran();
			tracker.SleepBetweenRetries = 0;
			ScanPartitions();
		}

		private void ScanPartitions()
		{
			ulong taskId = RandomShift.ThreadLocalInstance.NextLong();
			List<NodePartitions> nodePartitionsList = tracker.AssignPartitionsToNodes(cluster, ns);

			AsyncScanPartition[] tasks = new AsyncScanPartition[nodePartitionsList.Count];
			int count = 0;

			foreach (NodePartitions nodePartitions in nodePartitionsList)
			{
				tasks[count++] = new AsyncScanPartition(this, cluster, policy, listener, ns, setName, binNames, taskId, tracker, nodePartitions);
			}
			Execute(tasks, policy.maxConcurrentNodes);
		}

		protected internal override void OnSuccess()
		{
			try
			{
				if (tracker.IsClusterComplete(cluster, policy))
				{
					listener.OnSuccess();
					return;
				}

				// Prepare for retry.
				Reset();
				ScanPartitions();
			}
			catch (AerospikeException ae)
			{
				OnFailure(ae);
			}
			catch (Exception e)
			{
				OnFailure(new AerospikeException(e));
			}
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			tracker.PartitionError();
			ae.Iteration = tracker.iteration;
			listener.OnFailure(ae);
		}
	}
}
