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
	public class ReadCommand : SyncReadCommand
	{
		private readonly string[] binNames;
		private readonly bool isOperation;
		private Record record;

		public ReadCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, policy, key)
		{
			this.binNames = null;
			this.isOperation = false;
		}

		public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames)
			: base(cluster, policy, key)
		{
			this.binNames = binNames;
			this.isOperation = false;
		}

		public ReadCommand(Cluster cluster, Policy policy, Key key, bool isOperation)
			: base(cluster, policy, key)
		{
			this.binNames = null;
			this.isOperation = isOperation;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override void ParseResult(Connection conn)
		{
			ParseHeader(conn);
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				(record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}
		public Record Record
		{
			get
			{
				return record;
			}
		}
	}
}
