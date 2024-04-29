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
	public sealed class QueryRecordCommand : MultiCommand
	{
		private readonly Statement statement;
		private readonly RecordSet recordSet;
		private readonly ulong taskId;

		public QueryRecordCommand
		(
			Cluster cluster,
			Node node,
			QueryPolicy policy,
			Statement statement,
			ulong taskId,
			RecordSet recordSet,
			ulong clusterKey,
			bool first
		) : base(cluster, policy, node, statement.ns, clusterKey, first)
		{
			this.statement = statement;
			this.taskId = taskId;
			this.recordSet = recordSet;
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.QUERY;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(cluster, policy, statement, taskId, false, null);
		}

		protected internal override bool ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, out bval);

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (!recordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}
			return true;
		}
	}
}
