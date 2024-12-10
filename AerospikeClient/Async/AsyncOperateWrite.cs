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

using System;

namespace Aerospike.Client
{
	public sealed class AsyncOperateWrite : AsyncWriteBase
	{
		private readonly RecordListener listener;
		private readonly OperateArgs args;
		private Record record;

		public AsyncOperateWrite(AsyncCluster cluster, RecordListener listener, Key key, OperateArgs args)
			: base(cluster, args.writePolicy, key)
		{
			this.listener = listener;
			this.args = args;
		}

		public AsyncOperateWrite(AsyncOperateWrite other)
			: base(other)
		{
			this.args = other.args;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncOperateWrite(this);
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(args.writePolicy, Key, args);
		}

		protected internal override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, Key, true);

			if (resultCode == ResultCode.OK)
			{
				(this.record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, true);
				return true;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return true;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(Key, record);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
