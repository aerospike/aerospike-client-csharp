/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ReadHeaderCommand : SyncCommand
	{
		private readonly Policy policy;
		private readonly Key key;
		private readonly Partition partition;
		private Record record;

		public ReadHeaderCommand(Cluster cluster, Policy policy, Key key) 
		{
			this.policy = policy;
			this.key = key;
			this.partition = Partition.Read(cluster, policy, key);
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeRead(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, 14);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 18);
				record = new Record(null, generation, expiration);
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					record = null;
				}
				else
				{
					throw new AerospikeException(resultCode);
				}
			}
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
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
