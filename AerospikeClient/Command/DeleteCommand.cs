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
	public sealed class DeleteCommand : SyncCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly Key key;
		private readonly Partition partition;
		private bool existed;

		public DeleteCommand(Cluster cluster, WritePolicy writePolicy, Key key)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.key = key;
			this.partition = Partition.Write(cluster, writePolicy, key);
			cluster.AddTran();
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode()
		{
			return partition.GetNodeWrite(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.WRITE;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(writePolicy, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
			conn.UpdateLastUsed();

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
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

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryWrite(timeout);
			return true;
		}

		public bool Existed()
		{
			return existed;
		}
	}
}
