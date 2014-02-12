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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public class ReadCommand : SingleCommand
	{
		private readonly Policy policy;
		private readonly string[] binNames;
		private Record record;

		public ReadCommand(Cluster cluster, Policy policy, Key key, string[] binNames) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
			this.binNames = binNames;
		}

		protected internal sealed override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(key, binNames);
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
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					return;
				}

				if (resultCode == ResultCode.UDF_BAD_RESPONSE)
				{
					record = ParseRecord(opCount, fieldCount, generation, expiration);
					HandleUdfError(resultCode);
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

			if (record.bins.TryGetValue("FAILURE", out obj))
			{
				string ret = (string)obj;
				string[] list;
				string message;
				int code;

				try
				{
					list = ret.Split(':');
					code = Convert.ToInt32(list[2].Trim());
					message = list[0] + ':' + list[1] + ' ' + list[3];
				}
				catch (Exception)
				{
					// Use generic exception if parse error occurs.
					throw new AerospikeException(resultCode, ret);
				}

				throw new AerospikeException(code, message);
			}
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
				bins[name] = value;
			}
			return new Record(bins, generation, expiration);
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
