/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	public sealed class AsyncQuery : AsyncMultiCommand
	{
		private readonly AsyncMultiExecutor parent;
		private readonly RecordSequenceListener listener;
		private readonly Statement statement;
		private readonly ulong taskId;

		public AsyncQuery
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			ulong taskId
		) : base(cluster, policy, node, policy.socketTimeout, policy.totalTimeout, statement.Namespace)
		{
			this.parent = parent;
			this.listener = listener;
			this.statement = statement;
			this.taskId = taskId;
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.QUERY;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(cluster, policy, statement, taskId, false, null);
		}

		protected internal override void ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, out bval);

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return null;
		}

		protected internal override void OnSuccess()
		{
			parent.ChildSuccess(node);
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			parent.ChildFailure(e);
		}
	}
}
