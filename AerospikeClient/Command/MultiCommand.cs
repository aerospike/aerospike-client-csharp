/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
		protected internal int receiveOffset;

		protected internal MultiCommand(Node node)
		{
			this.node = node;
			this.receiveBuffer = new byte[2048];
		}

		protected internal sealed override Node GetNode()
		{
			return node;
		}

		protected internal void WriteHeader(int readAttr, int fieldCount, int operationCount)
		{
			// Write all header data except total size which must be written last. 
			dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[9] = (byte)readAttr;

			for (int i = 10; i < 26; i++)
			{
				dataBuffer[i] = 0;
			}
			ByteUtil.ShortToBytes((ushort)fieldCount, dataBuffer, 26);
			ByteUtil.ShortToBytes((ushort)operationCount, dataBuffer, 28);
			dataOffset = MSG_TOTAL_HEADER_SIZE;
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

				long size = ByteUtil.BytesToLong(receiveBuffer, 0);
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
				int fieldlen = ByteUtil.BytesToInt(receiveBuffer, 0);
				ReadBytes(fieldlen);
				int fieldtype = receiveBuffer[0];
				int size = fieldlen - 1;

				if (fieldtype == FieldType.DIGEST_RIPE)
				{
					digest = new byte[size];
					Array.Copy(receiveBuffer, 1, digest, 0, size);
				}
				else if (fieldtype == FieldType.NAMESPACE)
				{
					ns = ByteUtil.Utf8ToString(receiveBuffer, 1, size);
				}
				else if (fieldtype == FieldType.TABLE)
				{
					setName = ByteUtil.Utf8ToString(receiveBuffer, 1, size);
				}
			}
			return new Key(ns, digest, setName);
		}

		protected internal void ReadBytes(int length)
		{
			if (length > receiveBuffer.Length)
			{
				// Corrupted data streams can result in a huge length.
				// Do a sanity check here.
				if (length > MAX_BUFFER_SIZE)
				{
					throw new System.ArgumentException("Invalid readBytes length: " + length);
				}
				receiveBuffer = new byte[length];
			}

			int pos = 0;

			while (pos < length)
			{
				int count = bis.Read(receiveBuffer, pos, length - pos);

				if (count < 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				pos += count;
			}
			receiveOffset += length;
		}

		protected internal abstract bool ParseRecordResults(int receiveSize);
	}
}
