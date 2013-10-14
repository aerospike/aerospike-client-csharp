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
	public sealed class BatchCommandGet : MultiCommand
	{
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly HashSet<string> binNames;
		private readonly Record[] records;

		public BatchCommandGet(Node node, Dictionary<Key, BatchItem> keyMap, HashSet<string> binNames, Record[] records) : base(node)
		{
			this.keyMap = keyMap;
			this.binNames = binNames;
			this.records = records;
		}

		/// <summary>
		/// Parse all results in the batch.  Add records to shared list.
		/// If the record was not found, the bins will be null.
		/// </summary>
		protected internal override bool ParseRecordResults(int receiveSize)
		{
			//Parse each message response and add it to the result array
			receiveOffset = 0;

			while (receiveOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = receiveBuffer[5];

				// The only valid server return codes are "ok" and "not found".
				// If other return codes are received, then abort the batch.
				if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
				{
					throw new AerospikeException(resultCode);
				}

				byte info3 = receiveBuffer[3];

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					return false;
				}

				int generation = ByteUtil.BytesToInt(receiveBuffer, 6);
				int expiration = ByteUtil.BytesToInt(receiveBuffer, 10);
				int fieldCount = ByteUtil.BytesToShort(receiveBuffer, 18);
				int opCount = ByteUtil.BytesToShort(receiveBuffer, 20);
				Key key = ParseKey(fieldCount);
				BatchItem item = keyMap[key];

				if (item != null)
				{
					if (resultCode == 0)
					{
						int index = item.Index;
						records[index] = ParseRecord(opCount, generation, expiration);
					}
				}
				else
				{
					if (Log.DebugEnabled())
					{
						Log.Debug("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest));
					}
				}
			}
			return true;
		}

		/// <summary>
		/// Parses the given byte buffer and populate the result object.
		/// Returns the number of bytes that were parsed from the given buffer.
		/// </summary>
		private Record ParseRecord(int opCount, int generation, int expiration)
		{
			Dictionary<string, object> bins = null;

			for (int i = 0 ; i < opCount; i++)
			{
				ReadBytes(8);
				int opSize = ByteUtil.BytesToInt(receiveBuffer, 0);
				byte particleType = receiveBuffer[5];
				byte nameSize = receiveBuffer[7];

				ReadBytes(nameSize);
				string name = ByteUtil.Utf8ToString(receiveBuffer, 0, nameSize);

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				ReadBytes(particleBytesSize);
				object value = ByteUtil.BytesToParticle(particleType, receiveBuffer, 0, particleBytesSize);

				// Currently, the batch command returns all the bins even if a subset of
				// the bins are requested. We have to filter it on the client side.
				// TODO: Filter batch bins on server!
				if (binNames == null || binNames.Contains(name))
				{
					if (bins == null)
					{
						bins = new Dictionary<string, object>();
					}
					bins[name] = value;
				}
			}
			return new Record(bins, generation, expiration);
		}
	}
}