/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	public sealed class AsyncQueryPartitionExecutor : AsyncMultiExecutor
	{
		private readonly QueryPolicy policy;
		private readonly RecordSequenceListener listener;
		private readonly Statement statement;
		private readonly PartitionTracker tracker;
		private ulong taskId;

		public AsyncQueryPartitionExecutor
		(
			AsyncCluster cluster,
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			PartitionTracker tracker
		) : base(cluster)
		{
			this.policy = policy;
			this.listener = listener;
			this.statement = statement;
			this.tracker = tracker;

			tracker.SleepBetweenRetries = 0;
			taskId = statement.PrepareTaskId();
			QueryPartitions();
		}

		private void QueryPartitions()
		{
			List<NodePartitions> nodePartitionsList = tracker.AssignPartitionsToNodes(cluster, statement.ns);

			AsyncQueryPartition[] tasks = new AsyncQueryPartition[nodePartitionsList.Count];
			int count = 0;

			foreach (NodePartitions nodePartitions in nodePartitionsList)
			{
				tasks[count++] = new AsyncQueryPartition(this, cluster, policy, listener, statement, taskId, tracker, nodePartitions);
			}
			Execute(tasks, policy.maxConcurrentNodes);
		}

		protected internal override void OnSuccess()
		{
			try
			{
				if (tracker.IsComplete(cluster, policy))
				{
					listener.OnSuccess();
					return;
				}

				// Prepare for retry.
				Reset();
				taskId = RandomShift.ThreadLocalInstance.NextLong();
				QueryPartitions();
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
			ae.Iteration = tracker.iteration;
			listener.OnFailure(ae);
		}
	}
}
