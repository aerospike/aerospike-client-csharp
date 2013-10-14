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
	public sealed class BatchCommandExists : MultiCommand
	{
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly bool[] existsArray;

		public BatchCommandExists(Node node, Dictionary<Key, BatchItem> keyMap, bool[] existsArray) : base(node)
		{
			this.keyMap = keyMap;
			this.existsArray = existsArray;
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

				int fieldCount = ByteUtil.BytesToShort(receiveBuffer, 18);
				int opCount = ByteUtil.BytesToShort(receiveBuffer, 20);

				if (opCount > 0)
				{
					throw new AerospikeException.Parse("Received bins that were not requested!");
				}

				Key key = ParseKey(fieldCount);
				BatchItem item = keyMap[key];

				if (item != null)
				{
					int index = item.Index;
					existsArray[index] = resultCode == 0;
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
	}
}