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
	public sealed class ExistsCommand : SyncReadCommand
	{
		private bool exists;

		public ExistsCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, policy, key)
		{
		}

		protected internal override void WriteBuffer()
		{
			SetExists(policy, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			ParseHeader(conn);
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				exists = true;
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				exists = false;
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				exists = true;
				return;
			}

			throw new AerospikeException(resultCode);
		}

		public bool Exists()
		{
			return exists;
		}
	}
}
