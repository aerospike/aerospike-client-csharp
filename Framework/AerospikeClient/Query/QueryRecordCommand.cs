/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	public sealed class QueryRecordCommand : MultiCommand
	{
		private readonly Statement statement;
		private readonly RecordSet recordSet;

		public QueryRecordCommand
		(
			Cluster cluster,
			Node node,
			QueryPolicy policy,
			Statement statement,
			RecordSet recordSet,
			ulong clusterKey,
			bool first
		) : base(cluster, policy, node, statement.ns, clusterKey, first)
		{
			this.statement = statement;
			this.recordSet = recordSet;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(policy, statement, false, null);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord(key);

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (!recordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}
		}
	}
}
