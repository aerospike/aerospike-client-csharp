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
	public sealed class TxnMarkRollForward : SyncWriteCommand
	{
		private readonly Txn txn;

		public TxnMarkRollForward(Cluster cluster, Txn txn, WritePolicy writePolicy, Key key) 
			: base(cluster, writePolicy, key)
		{
			this.txn = txn;
		}

		protected internal override void WriteBuffer()
		{
			SetTxnMarkRollForward(txn, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			ParseHeader(conn);

			// BIN_EXISTS_ERROR is considered a success because it means a previous attempt already
			// succeeded in notifying the server that the MRT will be rolled forward.
			if (resultCode == ResultCode.OK || resultCode == ResultCode.BIN_EXISTS_ERROR)
			{
				return;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override void OnInDoubt()
		{
		}
	}
}
