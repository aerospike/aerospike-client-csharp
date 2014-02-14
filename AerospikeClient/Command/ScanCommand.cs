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
	public sealed class ScanCommand : MultiCommand
	{
		private readonly ScanPolicy policy;
		private readonly string ns;
		private readonly string setName;
		private readonly ScanCallback callback;
		private readonly string[] binNames;

		public ScanCommand
		(
			Node node, 
			ScanPolicy policy,
			string ns,
			string setName,
			ScanCallback callback,
			string[] binNames
		) : base(node)
		{
			this.policy = policy;
			this.ns = ns;
			this.setName = setName;
			this.callback = callback;
			this.binNames = binNames;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(policy, ns, setName, binNames);
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Read/parse remaining message bytes one record at a time.
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = dataBuffer[5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						return false;
					}
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

				// Parse bins.
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

					if (bins == null)
					{
						bins = new Dictionary<string, object>();
					}
					bins[name] = value;
				}

				if (!valid)
				{
					throw new AerospikeException.ScanTerminated();
				}

				// Call the callback function.
				callback(key, new Record(bins, generation, expiration));
			}
			return true;
		}
	}
}
