/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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