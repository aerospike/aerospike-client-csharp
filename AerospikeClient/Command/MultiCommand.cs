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
using System;
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
		protected internal volatile bool valid = true;

		protected internal MultiCommand(Node node)
		{
			this.node = node;
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
					status = ParseRecordResults(receiveSize);
				}
				else
				{
					status = false;
				}
			}
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

		protected internal abstract bool ParseRecordResults(int receiveSize);
	}
}
