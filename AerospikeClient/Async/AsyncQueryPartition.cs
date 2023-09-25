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
namespace Aerospike.Client
{
	public sealed class AsyncQueryPartition : AsyncMultiCommand
	{
		private readonly AsyncMultiExecutor parent;
		private readonly RecordSequenceListener listener;
		private readonly Statement statement;
		private readonly ulong taskId;
		private readonly PartitionTracker tracker;
		private readonly NodePartitions nodePartitions;

		public AsyncQueryPartition
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			ulong taskId,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		) : base(cluster, policy, (AsyncNode)nodePartitions.node, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.parent = parent;
			this.listener = listener;
			this.statement = statement;
			this.taskId = taskId;
			this.tracker = tracker;
			this.nodePartitions = nodePartitions;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(cluster, policy, statement, taskId, false, nodePartitions);
		}

		protected internal override void ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, dataBuffer, out bval);

			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (resultCode != 0)
				{
					tracker.PartitionUnavailable(nodePartitions, generation);
				}
				return;
			}

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();
			listener.OnRecord(key, record);
			tracker.SetLast(nodePartitions, key, bval);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return null;
		}

		protected internal override void OnSuccess()
		{
			parent.ChildSuccess(node);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			if (tracker.ShouldRetry(nodePartitions, ae))
			{
				parent.ChildSuccess(serverNode);
				return;
			}
			parent.ChildFailure(ae);
		}
	}
}
