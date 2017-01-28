/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
		private readonly Cluster cluster;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Partition partition;
		private bool existed;

		public DeleteCommand(Cluster cluster, WritePolicy policy, Key key) 
		{
			this.cluster = cluster;
			this.policy = policy;
			this.key = key;
			this.partition = new Partition(key);
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(policy, key);
		}

		protected internal override Node GetNode()
		{
			return cluster.GetMasterNode(partition);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
			{
				throw new AerospikeException(resultCode);
			}
			existed = resultCode == 0;
			EmptySocket(conn);
		}

		public bool Existed()
		{
			return existed;
		}
	}
}
