/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class BatchCommandGet : MultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly HashSet<string> binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchCommandGet
		(
			Node node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			Dictionary<Key, BatchItem> keyMap,
			HashSet<string> binNames,
			Record[] records,
			int readAttr
		) : base(node)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.keyMap = keyMap;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchGet(batchNamespace, binNames, readAttr);
		}

		/// <summary>
		/// Parse all results in the batch.  Add records to shared list.
		/// If the record was not found, the bins will be null.
		/// </summary>
		protected internal override bool ParseRecordResults(int receiveSize)
		{
			//Parse each message response and add it to the result array
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = dataBuffer[5];

				// The only valid server return codes are "ok" and "not found".
				// If other return codes are received, then abort the batch.
				if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
				{
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
				int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
				byte particleType = dataBuffer[5];
				byte nameSize = dataBuffer[7];

				ReadBytes(nameSize);
				string name = ByteUtil.Utf8ToString(dataBuffer, 0, nameSize);

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				ReadBytes(particleBytesSize);
				object value = ByteUtil.BytesToParticle(particleType, dataBuffer, 0, particleBytesSize);

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
