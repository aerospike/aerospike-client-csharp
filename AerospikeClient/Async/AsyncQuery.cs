/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
		private readonly QueryPolicy queryPolicy;
		private readonly RecordSequenceListener listener;
		private readonly Statement statement;

		public AsyncQuery
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			QueryPolicy queryPolicy,
			RecordSequenceListener listener,
			Statement statement
		) : base(parent, cluster, queryPolicy, node, true)
		{
			this.queryPolicy = queryPolicy;
			this.listener = listener;
			this.statement = statement;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(queryPolicy, statement, false);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}
	}
}
