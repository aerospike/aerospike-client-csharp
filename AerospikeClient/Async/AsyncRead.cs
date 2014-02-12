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
	public class AsyncRead : AsyncSingleCommand
	{
		private readonly Policy policy;
		private readonly RecordListener listener;
		private readonly string[] binNames;
		private Record record;

		public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, string[] binNames) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
			this.listener = listener;
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

		protected internal sealed override void ParseResult()
		{
			int resultCode = dataBuffer[5];
			int generation = ByteUtil.BytesToInt(dataBuffer, 6);
			int expiration = ByteUtil.BytesToInt(dataBuffer, 10);
			int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
			int opCount = ByteUtil.BytesToShort(dataBuffer, 20);
			dataOffset = Command.MSG_REMAINING_HEADER_SIZE;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					record = new Record(null, generation, expiration);
				}
				else
				{
					record = ParseRecord(opCount, fieldCount, generation, expiration);
				}
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					record = null;
				}
				else
				{
					throw new AerospikeException(resultCode);
				}
			}
		}

		private Record ParseRecord(int opCount, int fieldCount, int generation, int expiration)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			if (fieldCount > 0)
			{
				// Just skip over all the fields
				for (int i = 0; i < fieldCount; i++)
				{
					int fieldSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
					dataOffset += 4 + fieldSize;
				}
			}

			Dictionary<string, object> bins = null;

			for (int i = 0; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

				if (bins == null)
				{
					bins = new Dictionary<string, object>();
				}
				bins[name] = value;
			}
			return new Record(bins, generation, expiration);
		}

		protected internal sealed override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key, record);
			}
		}

		protected internal sealed override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
