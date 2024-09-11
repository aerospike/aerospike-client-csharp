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
	public sealed class AsyncTxnMarkRollForward : AsyncWriteBase
	{
		private readonly WriteListener listener;

		public AsyncTxnMarkRollForward
		(
			AsyncCluster cluster,
			WriteListener listener,
			WritePolicy writePolicy,
			Key key
		) : base(cluster, writePolicy, key)
		{
			this.listener = listener;
		}

		public AsyncTxnMarkRollForward(AsyncTxnMarkRollForward other)
			: base(other)
		{
			this.listener = other.listener;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncTxnMarkRollForward(this);
		}

		protected internal override void WriteBuffer()
		{
			SetTxnMarkRollForward(Key);
		}

		protected internal override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, Key, true);

			// BIN_EXISTS_ERROR is considered a success because it means a previous attempt already
			// succeeded in notifying the server that the MRT will be rolled forward.
			if (resultCode == ResultCode.OK || resultCode == ResultCode.BIN_EXISTS_ERROR)
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

