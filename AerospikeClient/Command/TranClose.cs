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
	public sealed class TranClose : SyncWriteCommand
	{
		private readonly Tran tran;

		public TranClose(Cluster cluster, Tran tran, WritePolicy writePolicy, Key key) 
			: base(cluster, writePolicy, key)
		{
			this.tran = tran;
		}

		protected internal override void WriteBuffer()
		{
			SetTranClose(tran, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			int resultCode = ParseHeader(conn);

			if (resultCode == ResultCode.OK || resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return;
			}

			throw new AerospikeException(resultCode);
		}
	}
}
