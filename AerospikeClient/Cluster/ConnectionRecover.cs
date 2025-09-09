/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Class used to recover sync connections based on timeoutDelay
	/// </summary>
	public sealed class ConnectionRecover
	{
		private readonly Connection conn;
		private readonly Node node;
		private byte[] headerBuf;
		private readonly int timeoutDelay;
		private int offset;
		private int length;
		private readonly bool isSingle;
		private readonly bool checkReturnCode;
		private bool lastGroup;
		private byte state;

		public ConnectionRecover(Connection conn, Node node, int timeoutDelay, Connection.ReadTimeout crt, bool isSingle)
		{
			this.conn = conn;
			this.node = node;
			this.timeoutDelay = timeoutDelay;
			this.isSingle = isSingle;
			this.offset = crt.offset;
			lastGroup = false;

			try
			{
				switch (crt.state)
				{
					case Command.STATE_READ_AUTH_HEADER:
						this.length = 10;
						this.isSingle = true;
						this.checkReturnCode = true;
						this.state = Command.STATE_READ_HEADER;

						if (offset >= length)
						{
							if (crt.buffer[length - 1] != 0)
							{
								// Authentication failed.
								Log.Debug(node.cluster.context, "Invalid user/password");
								Abort();
								return;
							}
							length = GetSize(crt.buffer) - (offset - 8);
							offset = 0;
							state = Command.STATE_READ_DETAIL;
						}
						else if (offset > 0)
						{
							CopyHeaderBuf(crt.buffer);
						}
						break;

					case Command.STATE_READ_HEADER:
						// Extend header length to 12 for multi-record responses to include
						// last group info3 bit at offset 11.
						this.length = isSingle ? 8 : 12;
						this.isSingle = isSingle;
						this.checkReturnCode = false;
						this.state = crt.state;

						if (offset >= length)
						{
							ParseProto(crt.buffer, offset);
						}
						else if (offset > 0)
						{
							CopyHeaderBuf(crt.buffer);
						}
						break;

					case Command.STATE_READ_DETAIL:
					default:
						this.length = crt.length;
						this.isSingle = isSingle;
						this.checkReturnCode = false;
						this.state = crt.state;
						break;
				}

				conn.UpdateLastUsed();
				conn.SetTimeout(1);
			}
			catch (Exception e)
			{
				Log.Debug(node.cluster.context, "Error recovering sync connection: ");
				Log.Debug(e.Message + e.StackTrace);
				Abort();
			}
		}

		/// <summary>
		/// Drain connection.
		/// </summary>
		/// <returns>true if draining is complete.</returns>
		public bool Drain(byte[] buf)
		{
			try
			{
				if (isSingle)
				{
					if (state == Command.STATE_READ_HEADER)
					{
						DrainHeader(buf);
					}
					DrainDetail(buf);
					Recover();
					return true;
				}
				else
				{
					while (true)
					{
						if (state == Command.STATE_READ_HEADER)
						{
							DrainHeader(buf);
						}
						DrainDetail(buf);

						if (lastGroup)
						{
							break;
						}
						length = 12;
						offset = 0;
						state = Command.STATE_READ_HEADER;
					}
					Recover();
					return true;
				}
			}
			catch (SocketException se)
			{
				if (se.SocketErrorCode == SocketError.TimedOut && (DateTime.Now - conn.LastUsed).TotalMilliseconds >= timeoutDelay)
				{
					Abort();
					return true;
				}
				// Put back on queue for later draining.
				return false;
			}
			catch (Exception e)
			{
				// Forcibly close connection.
				Log.Debug(node.cluster.context, "Error recovering sync connection: ");
				Log.Debug(e.Message + e.StackTrace);
				Abort();
				return true;
			}
		}

		/// <summary>
		/// Has connection been recovered or closed.
		/// </summary>
		public bool IsComplete()
		{
			return state == Command.STATE_COMPLETE;
		}

		/// <summary>
		/// Close connection.
		/// </summary>
		public void Abort()
		{
			node.CloseConnection(conn);
			state = Command.STATE_COMPLETE;
		}

		private void Recover()
		{
			//Log.Debug(node.cluster.context, "Sync connection recovered");
			conn.UpdateLastUsed();
			node.PutConnection(conn);
			state = Command.STATE_COMPLETE;
		}

		private void DrainHeader(byte[] buf)
		{
			byte[] b = (offset == 0) ? buf : headerBuf;

			while (true)
			{
				int count = conn.Read(b, offset, length - offset);

				if (count < 0)
				{
					// Connection closed by server.
					throw new EndOfStreamException();
				}
				offset += count;

				if (offset >= length)
				{
					break;
				}

				// Partial read.
				if (b == buf)
				{
					// Convert to header buf.
					CopyHeaderBuf(b);
					b = headerBuf;
				}
			}

			if (checkReturnCode)
			{
				if (b[length - 1] != 0)
				{
					// Authentication failed.
					Log.Debug(node.cluster.context, "Invalid user/password");
					Abort();
					return;
				}
			}
			ParseProto(b, length);
		}

		private void DrainDetail(byte[] buf)
		{
			while (offset < length)
			{
				int rem = length - offset;
				int len = (rem <= buf.Length) ? rem : buf.Length;
				int count = conn.Read(buf, 0, len);

				if (count < 0)
				{
					// Connection closed by server.
					throw new EndOfStreamException();
				}
				offset += count;
			}
		}

		private void CopyHeaderBuf(byte[] buf)
		{
			if (headerBuf == null)
			{
				headerBuf = new byte[length];
			}

			for (int i = 0; i < offset; i++)
			{
				headerBuf[i] = buf[i];
			}
		}

		private int GetSize(byte[] buf)
		{
			long proto = ByteUtil.BytesToLong(buf, 0);
			return (int)(proto & 0xFFFFFFFFFFFFL);
		}

		private void ParseProto(byte[] buf, int bytesRead)
		{
			long proto = ByteUtil.BytesToLong(buf, 0);

			if (!isSingle)
			{
				// The last group trailer will never be compressed.
				bool compressed = ((ulong)(proto >> 48) & 0xff) == Command.MSG_TYPE_COMPRESSED;

				if (compressed)
				{
					// Do not recover connections with compressed data because that would
					// require saving large buffers with associated State and performing decompression
					// just to drain the connection.
					throw new AerospikeException("Recovering connections with compressed multi-record data is not supported");
				}

				// Warning: The following code assumes multi-record responses always end with a separate proto
				// that only contains one header with the info3 last group bit.  This is always true for batch
				// and scan, but query does not conform.  Therefore, connection recovery for queries will
				// likely fail.
				byte info3 = buf[length - 1];

				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					lastGroup = true;
				}
			}
			int size = (int)(proto & 0xFFFFFFFFFFFFL);
			length = size - (bytesRead - 8);
			offset = 0;
			state = Command.STATE_READ_DETAIL;
		}
	}
}
