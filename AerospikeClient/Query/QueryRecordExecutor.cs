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
	public sealed class QueryRecordExecutor : QueryExecutor
	{
		private readonly RecordSet recordSet;

		public QueryRecordExecutor(Cluster cluster, QueryPolicy policy, Statement statement, Node[] nodes)
			: base(cluster, policy, statement, nodes)
		{
			this.recordSet = new RecordSet(this, policy.recordQueueSize, cancel.Token);
			InitializeThreads();
		}

		public void Execute()
		{
			StartThreads();
		}

		protected internal override MultiCommand CreateCommand(Node node, ulong clusterKey, bool first)
		{
			return new QueryRecordCommand(cluster, node, policy, statement, taskId, recordSet, clusterKey, first);
		}

		protected internal override void SendCancel()
		{
			recordSet.Abort();
		}

		protected internal override void SendCompleted()
		{
			recordSet.Put(RecordSet.END);
		}

		public RecordSet RecordSet
		{
			get
			{
				return recordSet;
			}
		}
	}
}
