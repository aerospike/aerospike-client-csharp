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
	public sealed class TranAddKeys : SyncWriteCommand
	{
		private readonly OperateArgs args;

		public TranAddKeys (Cluster cluster, Key key, OperateArgs args) 
			: base(cluster, args.writePolicy, key)
		{
			this.args = args;
		}

		protected internal override void WriteBuffer()
		{
			SetTranAddKeys(args.writePolicy, key, args);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, 8, Command.STATE_READ_HEADER);

			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			SizeBuffer(receiveSize);
			conn.ReadFully(dataBuffer, receiveSize, Command.STATE_READ_DETAIL);
			conn.UpdateLastUsed();

			ulong type = (ulong)(sz >> 48) & 0xff;

			if (type == Command.AS_MSG_TYPE)
			{
				dataOffset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED)
			{
				int usize = (int)ByteUtil.BytesToLong(dataBuffer, 0);
				byte[] ubuf = new byte[usize];

				ByteUtil.Decompress(dataBuffer, 8, receiveSize, ubuf, usize);
				dataBuffer = ubuf;
				dataOffset = 13;
			}
			else
			{
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			int resultCode = dataBuffer[dataOffset] & 0xFF;
			dataOffset++;
			int generation = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			int expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 8;
			int fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			int opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int fieldType = dataBuffer[dataOffset++];
				int size = len - 1;

				if (fieldType == FieldType.MRT_DEADLINE)
				{
					int deadline = ByteUtil.LittleBytesToInt(dataBuffer, dataOffset);
					policy.Tran.Deadline = deadline;
				}
				dataOffset += size;
			}

			if (resultCode == ResultCode.OK)
			{
				return;
			}

			throw new AerospikeException(resultCode);
		}
	}
}
