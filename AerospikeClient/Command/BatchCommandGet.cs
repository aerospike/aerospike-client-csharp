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
	public sealed class BatchCommandGet : MultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly HashSet<string> binNames;
		private readonly Record[] records;
		private readonly int readAttr;
		private int index;

		public BatchCommandGet
		(
			Node node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			HashSet<string> binNames,
			Record[] records,
			int readAttr
		) : base(node)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
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
			SetBatchGet(policy, keys, batch, binNames, readAttr);
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
				int offset = batch.offsets[index++];

				if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
				{
					if (resultCode == 0)
					{
						records[offset] = ParseRecord(opCount, generation, expiration);
					}
				}
				else 
				{
					throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
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
				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

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
