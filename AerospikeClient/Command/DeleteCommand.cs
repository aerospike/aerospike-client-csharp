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
	public sealed class DeleteCommand : SyncWriteCommand
	{
		private bool existed;

		public DeleteCommand(Cluster cluster, WritePolicy writePolicy, Key key)
			: base(cluster, writePolicy, key)
		{
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(writePolicy, key);
		}

		protected internal override void ParseResult(Connection conn)
		{
			ParseHeader(conn);
			ParseFields(policy.Txn, key, true);

			if (resultCode == ResultCode.OK)
			{
				existed = true;
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				existed = false;
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (writePolicy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				existed = true;
				return;
			}

			throw new AerospikeException(resultCode);
		}

		public bool Existed()
		{
			return existed;
		}
	}
}
