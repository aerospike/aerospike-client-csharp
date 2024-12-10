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
	public sealed class AsyncWrite : AsyncWriteBase
	{
		private readonly WriteListener listener;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public AsyncWrite
		(
			AsyncCluster cluster,
			WritePolicy writePolicy,
			WriteListener listener,
			Key key,
			Bin[] bins,
			Operation.Type operation
		) : base(cluster, writePolicy, key)
		{
			this.listener = listener;
			this.bins = bins;
			this.operation = operation;
		}

		public AsyncWrite(AsyncWrite other)
			: base(other)
		{
			this.listener = other.listener;
			this.bins = other.bins;
			this.operation = other.operation;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncWrite(this);
		}

		protected internal override void WriteBuffer()
		{
			SetWrite(writePolicy, operation, Key, bins);
		}

		protected internal override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, Key, true);

			if (resultCode == ResultCode.OK)
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
				listener.OnSuccess(Key);
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

