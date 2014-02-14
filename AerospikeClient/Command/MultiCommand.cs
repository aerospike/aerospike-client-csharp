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

			for (int i = 0; i < fieldCount; i++)
			{
				ReadBytes(4);
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, 0);
				ReadBytes(fieldlen);
				int fieldtype = dataBuffer[0];
				int size = fieldlen - 1;

				if (fieldtype == FieldType.DIGEST_RIPE)
				{
					digest = new byte[size];
					Array.Copy(dataBuffer, 1, digest, 0, size);
				}
				else if (fieldtype == FieldType.NAMESPACE)
				{
					ns = ByteUtil.Utf8ToString(dataBuffer, 1, size);
				}
				else if (fieldtype == FieldType.TABLE)
				{
					setName = ByteUtil.Utf8ToString(dataBuffer, 1, size);
				}
			}
			return new Key(ns, digest, setName);
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
