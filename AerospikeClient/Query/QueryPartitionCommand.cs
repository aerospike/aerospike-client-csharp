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

namespace Aerospike.Client
{
	public sealed class QueryPartitionCommand : MultiCommand
	{
		private readonly Statement statement;
		private readonly ulong taskId;
		private readonly RecordSet recordSet;
		private readonly PartitionTracker tracker;
		private readonly NodePartitions nodePartitions;

		public QueryPartitionCommand
		(
			Cluster cluster,
			Policy policy,
			Statement statement,
			ulong taskId,
			RecordSet recordSet,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		) : base(cluster, policy, nodePartitions.node, statement.ns, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.statement = statement;
			this.taskId = taskId;
			this.recordSet = recordSet;
			this.tracker = tracker;
			this.nodePartitions = nodePartitions;
		}

		public override void Execute()
		{
			try
			{
				ExecuteCommand();
			}
			catch (AerospikeException ae)
			{
				if (!tracker.ShouldRetry(nodePartitions, ae))
				{
					throw ae;
				}
			}
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(cluster, policy, statement, taskId, false, nodePartitions);
		}

		protected internal override bool ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, out bval);

			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (resultCode != 0)
				{
					tracker.PartitionUnavailable(nodePartitions, generation);
				}
				return true;
			}

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (tracker.AllowRecord())
			{
				if (!recordSet.Put(new KeyRecord(key, record)))
				{
					Stop();
					throw new AerospikeException.QueryTerminated();
				}

				tracker.SetLast(nodePartitions, key, bval);
			}
			return true;
		}
	}
}
