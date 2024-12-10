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
	public class AsyncRead : AsyncReadBase
	{
		private readonly RecordListener listener;
		private readonly string[] binNames;
		private readonly bool isOperation;
		protected Record record;

		// Read constructor.
		public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, string[] binNames) 
			: base(cluster, policy, key)
		{
			this.listener = listener;
			this.binNames = binNames;
			this.isOperation = false;
		}

		// Operate constructor.
		public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, bool isOperation)
			: base(cluster, policy, key)
		{
			this.listener = listener;
			this.binNames = null;
			this.isOperation = isOperation;
		}

		public AsyncRead(AsyncRead other)
			: base(other)
		{
			this.listener = other.listener;
			this.binNames = other.binNames;
			this.isOperation = other.isOperation;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncRead(this);
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal sealed override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				(this.record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
				return true;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
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
				listener.OnSuccess(key, record);
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
