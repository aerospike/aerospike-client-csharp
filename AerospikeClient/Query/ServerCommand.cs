/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
	public sealed class ServerCommand : QueryCommand
	{
		private readonly WritePolicy writePolicy;

		public ServerCommand(Node node, WritePolicy policy, Statement statement) 
			: base(node, policy, statement)
		{
			this.writePolicy = policy;
		}

		protected internal override void WriteQueryHeader(int fieldCount, int operationCount)
		{
			WriteHeader(writePolicy, Command.INFO1_READ, Command.INFO2_WRITE, fieldCount, operationCount);
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Server commands (Query/Execute UDF) should only send back a return code.
			// Keep parsing logic to empty socket buffer just in case server does
			// send records back.
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = dataBuffer[5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						return false;
					}
					throw new AerospikeException(resultCode);
				}

				byte info3 = dataBuffer[3];

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					return false;
				}

				int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				int opCount = ByteUtil.BytesToShort(dataBuffer, 20);

				ParseKey(fieldCount);

				for (int i = 0 ; i < opCount; i++)
				{
					ReadBytes(8);
					int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
					byte nameSize = dataBuffer[7];

					ReadBytes(nameSize);

					int particleBytesSize = (int)(opSize - (4 + nameSize));
					ReadBytes(particleBytesSize);
				}

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}
			}
			return true;
		}
	}
}
