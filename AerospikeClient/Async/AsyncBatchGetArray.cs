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
	public sealed class AsyncBatchGetArray : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly HashSet<string> binNames;
		private readonly Record[] records;
		private readonly int readAttr;
		private int index;

		public AsyncBatchGetArray
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			HashSet<string> binNames,
			Record[] records,
			int readAttr
		) : base(parent, cluster, node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchGet(keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			int offset = batch.offsets[index++];

			if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
			{
				if (resultCode == 0)
				{
					records[offset] = ParseRecord();
				}
			}
			else
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
				}
			}	
		}
	}
}
