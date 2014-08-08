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
				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

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
