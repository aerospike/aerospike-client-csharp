/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
	public sealed class AsyncBatchGetSequence : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;

		public AsyncBatchGetSequence
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			Key[] keys,
			HashSet<string> binNames,
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, node, false, binNames)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.keys = keys;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchGet(policy, keys, batchNamespace, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			if (resultCode == 0)
			{
				Record record = ParseRecordBatch();
				listener.OnRecord(key, record);
			}
			else
			{
				listener.OnRecord(key, null);
			}
		}
	}
}
