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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class QueryPartitionCommand : MultiCommand
	{
		private readonly Statement statement;
		private readonly RecordSet recordSet;
		private readonly PartitionTracker tracker;
		private readonly NodePartitions nodePartitions;

		public QueryPartitionCommand
		(
			Cluster cluster,
			Node node,
			Policy policy,
			Statement statement,
			RecordSet recordSet,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		) : base(cluster, policy, nodePartitions.node, statement.ns, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.statement = statement;
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
				if (!tracker.ShouldRetry(ae))
				{
					throw ae;
				}
			}
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(policy, statement, false, nodePartitions);
		}

		protected internal override void ParseRow(Key key)
		{
			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				tracker.PartitionDone(nodePartitions, generation);
				return;
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			tracker.SetDigest(nodePartitions, key);

			if (!recordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}
		}
	}
}
