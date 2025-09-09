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
	public sealed class AsyncTxnClose : AsyncWriteBase
	{
		private readonly Txn txn;
		private readonly DeleteListener listener;

		public AsyncTxnClose
		(
			AsyncCluster cluster,
			Txn txn,
			DeleteListener listener,
			WritePolicy writePolicy,
			Key key
		) : base(cluster, writePolicy, key)
		{
			this.txn = txn;
			this.listener = listener;
		}

		public AsyncTxnClose(AsyncTxnClose other)
			: base(other)
		{
			this.txn = other.txn;
			this.listener = other.listener;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncTxnClose(this);
		}

		protected internal override void WriteBuffer()
		{
			SetTxnClose(txn, Key);
		}

		protected internal override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, Key, true);

			if (resultCode == ResultCode.OK || resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return true;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override void OnInDoubt()
		{
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(Key, true);
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

