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
	public sealed class BatchCommandExists : MultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly bool[] existsArray;

		public BatchCommandExists
		(
			Node node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			Dictionary<Key, BatchItem> keyMap,
			bool[] existsArray
		) : base(node)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.keyMap = keyMap;
			this.existsArray = existsArray;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchExists(batchNamespace);
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

				int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				int opCount = ByteUtil.BytesToShort(dataBuffer, 20);

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
