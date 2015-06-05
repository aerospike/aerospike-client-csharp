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
	public sealed class BatchCommandNodeExists : MultiCommand
	{
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;
		private int index;

		public BatchCommandNodeExists
		(
			Node node,
			Policy policy,
			Key[] keys,
			bool[] existsArray
		) : base(node)
		{
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchExists(policy, keys);
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

				if (! Util.ByteArrayEquals(key.digest, keys[index].digest))
				{
					Log.Warn("Returned batch keys not in same order.");
				}

				existsArray[index] = resultCode == 0;
				index++;
			}
			return true;
		}
	}
}
