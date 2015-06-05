/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class QueryRecordCommand : QueryCommand
	{
		private readonly RecordSet recordSet;

		public QueryRecordCommand(Node node, Policy policy, Statement statement, RecordSet recordSet) 
			: base(node, policy, statement)
		{
			this.recordSet = recordSet;
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Read/parse remaining message bytes one record at a time.
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

				int generation = ByteUtil.BytesToInt(dataBuffer, 6);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 10);
				int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				int opCount = ByteUtil.BytesToShort(dataBuffer, 20);

				Key key = ParseKey(fieldCount);

				// Parse bins.
				Dictionary<string, object> bins = null;

				for (int i = 0 ; i < opCount; i++)
				{
					ReadBytes(8);
					int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
					byte particleType = dataBuffer[5];
					byte nameSize = dataBuffer[7];

					ReadBytes(nameSize);
					string name = ByteUtil.Utf8ToString(dataBuffer, 0, nameSize);

					int particleBytesSize = (int)(opSize - (4 + nameSize));
					ReadBytes(particleBytesSize);
					object value = ByteUtil.BytesToParticle(particleType, dataBuffer, 0, particleBytesSize);

					if (bins == null)
					{
						bins = new Dictionary<string, object>();
					}
					bins[name] = value;
				}

				Record record = new Record(bins, generation, expiration);

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

				if (!recordSet.Put(new KeyRecord(key, record)))
				{
					Stop();
					throw new AerospikeException.QueryTerminated();
				}
			}
			return true;
		}
	}
}
