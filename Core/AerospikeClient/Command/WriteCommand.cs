/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
	public sealed class WriteCommand : SyncCommand
	{
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public WriteCommand(WritePolicy policy, Key key, Bin[] bins, Operation.Type operation) 
		{
			this.policy = policy;
			this.key = key;
			this.bins = bins;
			this.operation = operation;
		}

		protected internal override void WriteBuffer()
		{
			SetWrite(policy, operation, key, bins);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}
			EmptySocket(conn);
		}
	}
}
