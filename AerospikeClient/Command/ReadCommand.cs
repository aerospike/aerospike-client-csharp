/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public class ReadCommand : SyncCommand
	{
		protected internal readonly Cluster cluster;
		private readonly Policy policy;
		protected internal readonly Key key;
		protected internal readonly Partition partition;
		private readonly string[] binNames;
		private Record record;

		public ReadCommand(Cluster cluster, Policy policy, Key key, string[] binNames) 
		{
			this.cluster = cluster;
			this.policy = policy;
			this.key = key;
			this.partition = new Partition(key);
			this.binNames = binNames;
		}

		protected internal sealed override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override Node GetNode()
		{
			return GetReadNode(cluster, partition, policy.replica);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			byte headerLength = dataBuffer[8];
			int resultCode = dataBuffer[13];
			int generation = ByteUtil.BytesToInt(dataBuffer, 14);
			int expiration = ByteUtil.BytesToInt(dataBuffer, 18);
			int fieldCount = ByteUtil.BytesToShort(dataBuffer, 26); // almost certainly 0
			int opCount = ByteUtil.BytesToShort(dataBuffer, 28);
			int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

			// Read remaining message bytes.
			if (receiveSize > 0)
			{
				SizeBuffer(receiveSize);
				conn.ReadFully(dataBuffer, receiveSize);
			}

			if (resultCode != 0)
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.LARGE_ITEM_NOT_FOUND)
				{
					return;
				}

				if (resultCode == ResultCode.UDF_BAD_RESPONSE)
				{
					record = ParseRecord(opCount, fieldCount, generation, expiration);
					HandleUdfError(resultCode);
					return;
				}
				throw new AerospikeException(resultCode);
			}

			if (opCount == 0)
			{
				// Bin data was not returned.
				record = new Record(null, generation, expiration);
				return;
			}
			record = ParseRecord(opCount, fieldCount, generation, expiration);
		}

		private void HandleUdfError(int resultCode)
		{
			object obj;

			if (!record.bins.TryGetValue("FAILURE", out obj))
			{
				throw new AerospikeException(resultCode);
			}

			string ret = (string)obj;
			string message;
			int code;

			try
			{
				string[] list = ret.Split(':');
				code = Convert.ToInt32(list[2].Trim());

				if (code == ResultCode.LARGE_ITEM_NOT_FOUND)
				{
					record = null;
					return;
				}
				message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception)
			{
				// Use generic exception if parse error occurs.
				throw new AerospikeException(resultCode, ret);
			}

			throw new AerospikeException(code, message);
		}

		private Record ParseRecord(int opCount, int fieldCount, int generation, int expiration)
		{
			Dictionary<string, object> bins = null;
			int receiveOffset = 0;

			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			if (fieldCount != 0)
			{
				// Just skip over all the fields
				for (int i = 0; i < fieldCount; i++)
				{
					int fieldSize = ByteUtil.BytesToInt(dataBuffer, receiveOffset);
					receiveOffset += 4 + fieldSize;
				}
			}

			for (int i = 0 ; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, receiveOffset);
				byte particleType = dataBuffer[receiveOffset + 5];
				byte nameSize = dataBuffer[receiveOffset + 7];
				string name = ByteUtil.Utf8ToString(dataBuffer, receiveOffset + 8, nameSize);
				receiveOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle(particleType, dataBuffer, receiveOffset, particleBytesSize);
				receiveOffset += particleBytesSize;

				if (bins == null)
				{
					bins = new Dictionary<string, object>();
				}
				AddBin(bins, name, value);
			}
			return new Record(bins, generation, expiration);
		}

		protected internal virtual void AddBin(Dictionary<string, object> bins, string name, object value)
		{
			bins[name] = value;
		}

		public Record Record
		{
			get
			{
				return record;
			}
		}
	}
}
