/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	public abstract class AsyncSingleCommand : AsyncCommand
	{
		protected int resultCode;
		protected int generation;
		protected int expiration;
		protected int fieldCount;
		protected int opCount;

		public AsyncSingleCommand(AsyncCluster cluster, Policy policy) 
			: base(cluster, policy)
		{
		}

		public AsyncSingleCommand(AsyncSingleCommand other)
			: base(other)
		{
		}
		
		protected internal sealed override void ParseCommand()
		{
			ParseResult();
			Finish();
		}

		protected void ParseHeader()
		{
			resultCode = dataBuffer[dataOffset + 5];
			generation = ByteUtil.BytesToInt(dataBuffer, dataOffset + 6);
			expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset + 10);
			fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 18);
			opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 20);
			dataOffset += Command.MSG_REMAINING_HEADER_SIZE;
		}

		protected void ParseFields(Txn txn, Key key, bool hasWrite)
		{
			if (txn == null)
			{
				SkipFields(fieldCount);
				return;
			}

			long? version = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.RECORD_VERSION)
				{
					if (size == 7)
					{
						version = ByteUtil.VersionBytesToLong(dataBuffer, dataOffset);
					}
					else
					{
						throw new AerospikeException("Record version field has invalid size: " + size);
					}
				}
				dataOffset += size;
			}

			if (hasWrite)
			{
				txn.OnWrite(key, version, resultCode);
			}
			else
			{
				txn.OnRead(key, version);
			}
		}

		protected void SkipFields(int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		protected void ParseTxnDeadline(Txn txn)
		{
			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.MRT_DEADLINE)
				{
					int deadline = ByteUtil.LittleBytesToInt(dataBuffer, dataOffset);
					txn.Deadline = deadline;
				}
				dataOffset += size;
			}
		}

		protected internal abstract bool ParseResult();
	}
}
