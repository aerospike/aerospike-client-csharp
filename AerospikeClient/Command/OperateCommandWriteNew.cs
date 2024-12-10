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

using System.Buffers;

namespace Aerospike.Client
{
	internal class OperateCommandWriteNew : WriteCommandNew, ICommand
	{
		private readonly OperateArgs args;
		public Record Record { get; private set; }

		public OperateCommandWriteNew(ArrayPool<byte> bufferPool, Cluster cluster, Key key, OperateArgs args)
			: base(bufferPool, cluster, args.writePolicy, key)
		{
			this.args = args;
		}

		public new void WriteBuffer()
		{
			this.SetOperate(args.writePolicy, key, args);
		}

		public new async Task ParseResult(IConnection conn, CancellationToken token)
		{
			await this.ParseHeader(conn, token);
			this.ParseFields(Policy.Txn, key, true);

			if (ResultCode == Client.ResultCode.OK)
			{
				(this.Record, DataOffset) = Policy.recordParser.ParseRecord(DataBuffer, DataOffset, OpCount, Generation, Expiration, true);
				return;
			}

			if (ResultCode == Client.ResultCode.FILTERED_OUT)
			{
				if (Policy.failOnFilteredOut)
				{
					throw new AerospikeException(ResultCode);
				}
				return;
			}

			throw new AerospikeException(ResultCode);
		}

		public void HandleNotFound(int resultCode)
		{
			// Only throw not found exception for command with write operations.
			// Read-only command operations return a null record.
			if (args.hasWrite)
			{
				throw new AerospikeException(resultCode);
			}
		}

		public new bool PrepareRetry(bool timeout)
		{
			throw new NotImplementedException();
			/*if (args.hasWrite)
			{
				partition.PrepareRetryWrite(timeout);
			}
			else
			{
				partition.PrepareRetryRead(timeout);
			}
			return true;*/
		}
	}
}
