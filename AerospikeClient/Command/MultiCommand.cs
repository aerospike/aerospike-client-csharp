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
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Aerospike.Client
{
	public abstract class MultiCommand : SyncCommand
	{
		private const int MAX_BUFFER_SIZE = 1024 * 1024 * 10; // 10 MB

		private BufferedStream bis;
		protected internal readonly Node node;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int batchIndex;
		protected internal int fieldCount;
		protected internal int opCount;
		private readonly bool stopOnNotFound;
		protected internal volatile bool valid = true;

		protected internal MultiCommand(Node node, bool stopOnNotFound)
		{
			this.node = node;
			this.stopOnNotFound = stopOnNotFound;
		}

		protected internal sealed override Node GetNode()
		{
			return node;
		}

		protected internal sealed override void ParseResult(Connection conn)
		{
			// Read socket into receive buffer one record at a time.  Do not read entire receive size
			// because the thread local receive buffer would be too big.  Also, scan callbacks can nest 
			// further database commands which contend with the receive buffer.
			Stream nis = new NetworkStream(conn.Socket);
			bis = new BufferedStream(nis, 8192);
			bool status = true;

			while (status)
			{
				// Read header.
				ReadBytes(8);

				long size = ByteUtil.BytesToLong(dataBuffer, 0);
				int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL));

				if (receiveSize > 0)
				{
					status = ParseGroup(receiveSize);
				}
			}
		}

		private bool ParseGroup(int receiveSize)
		{
			//Parse each message response and add it to the result array
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				resultCode = dataBuffer[5] & 0xFF;

				// The only valid server return codes are "ok" and "not found".
				// If other return codes are received, then abort the batch.
				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						if (stopOnNotFound)
						{
							return false;
						}
					}
					else
					{
						throw new AerospikeException(resultCode);
					}
				}

				byte info3 = dataBuffer[3];

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					return false;
				}

				generation = ByteUtil.BytesToInt(dataBuffer, 6);
				expiration = ByteUtil.BytesToInt(dataBuffer, 10);
				batchIndex = ByteUtil.BytesToInt(dataBuffer, 14);
				fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				opCount = ByteUtil.BytesToShort(dataBuffer, 20);

				Key key = ParseKey(fieldCount);
				ParseRow(key);
			}
			return true;
		}
		
		protected internal Key ParseKey(int fieldCount)
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;

			for (int i = 0; i < fieldCount; i++)
			{
				ReadBytes(4);
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, 0);
				ReadBytes(fieldlen);
				int fieldtype = dataBuffer[0];
				int size = fieldlen - 1;

				switch (fieldtype) 
				{
				case FieldType.DIGEST_RIPE:
					digest = new byte[size];
					Array.Copy(dataBuffer, 1, digest, 0, size);
					break;
			
				case FieldType.NAMESPACE:
					ns = ByteUtil.Utf8ToString(dataBuffer, 1, size);
					break;
				
				case FieldType.TABLE:
					setName = ByteUtil.Utf8ToString(dataBuffer, 1, size);
					break;

				case FieldType.KEY:
					userKey = ByteUtil.BytesToKeyValue(dataBuffer[1], dataBuffer, 2, size-1);
					break;
				}
			}
			return new Key(ns, digest, setName, userKey);
		}

		protected internal Record ParseRecord()
		{
			Dictionary<string, object> bins = null;

			for (int i = 0; i < opCount; i++)
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
			return new Record(bins, generation, expiration);
		}
		
		protected internal void ReadBytes(int length)
		{
			if (length > dataBuffer.Length)
			{
				// Corrupted data streams can result in a huge length.
				// Do a sanity check here.
				if (length > MAX_BUFFER_SIZE)
				{
					throw new System.ArgumentException("Invalid readBytes length: " + length);
				}
				dataBuffer = new byte[length];
			}

			int pos = 0;

			while (pos < length)
			{
				int count = bis.Read(dataBuffer, pos, length - pos);

				if (count <= 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				pos += count;
			}
			dataOffset += length;
		}

		public void Stop()
		{
			valid = false;
		}

		public bool IsValid()
		{
			return valid;
		}

		protected internal abstract void ParseRow(Key key);
	}
}
