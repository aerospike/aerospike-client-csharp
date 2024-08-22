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

using Aerospike.Client;

namespace Aerospike.Client
{
	public sealed class OperateCommandWrite : SyncWriteCommand
	{
		private readonly OperateArgs args;
		public Record Record { get; private set; }

		public OperateCommandWrite(Cluster cluster, Key key, OperateArgs args)
			: base(cluster, args.writePolicy, key)
		{
			this.args = args;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(args.writePolicy, key, args);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			ParseHeader(conn);
			ParseFields(policy.Txn, key, true);

			if (resultCode == ResultCode.OK) {
				Record = policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, true);
				return;
			}

			if (opCount > 0) {
				throw new AerospikeException("Unexpected operate opCount on error: " + opCount + ',' + resultCode);
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}
		
	}
}
