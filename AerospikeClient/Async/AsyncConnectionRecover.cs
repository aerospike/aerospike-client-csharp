/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	/// 
	/// </summary>
	public sealed class AsyncConnectionRecover : ITimeout, IAsyncCommand
	{
		private readonly Log.Context logContext;
		private readonly AsyncNode node;
		private readonly AsyncConnection conn;
		private readonly int timeoutDelay;
		private int offset;
		private int length;
		private byte[] dataBuffer;
		private bool inAuth;
		private bool inHeader;
		private bool isSingle;
		private volatile bool done;
		private long proto;
		private ValueStopwatch watch;

		public AsyncConnectionRecover(AsyncCommand cmd, AsyncConnection conn, bool inAuth, bool inHeader, bool isSingle)
		{
			Log.Debug(cmd.cluster.context, "Creating AsyncConnectionRecover");
			this.logContext = cmd.cluster.context;
			this.node = cmd.node;
			this.conn = conn;
			this.inAuth = inAuth;
			this.inHeader = inHeader;
			this.isSingle = isSingle;
			this.offset = cmd.dataOffset;
			this.timeoutDelay = cmd.policy.TimeoutDelay;
			done = false;
			watch = ValueStopwatch.StartNew();

			if (!inHeader)
			{
				dataBuffer = new byte[8];
			}
            else
            {
				proto = ByteUtil.BytesToLong(cmd.dataBuffer, cmd.dataOffset);
				length = (int)(proto & 0xFFFFFFFFFFFFL);
				dataBuffer = new byte[length];
            }
        }

		public bool CheckTimeout()
		{
			if (done)
			{
				return false; // Finished with the recovery or errored out. Do not put back on timeout queue.
			}

			if (watch.IsActive && watch.ElapsedMilliseconds >= timeoutDelay)
			{
				Abort();
				Log.Debug(logContext, "Recovery async timed out");
				return false; // Do not put back on timeout queue.
			}

			return true; // Timeout not reached, continue recovery process
		}

		/// <summary>
		/// Close connection.
		/// </summary>
		public void Abort()
		{
			done = true;
			node.CloseAsyncConn(conn);
		}

		public void StartDrain()
		{
			conn.ChangeCommand(this);
		}

		public void ReceiveComplete()
		{
			Drain();
		}

		public void Drain()
		{
			try
			{
				if (inHeader)
				{
					proto = ByteUtil.BytesToLong(dataBuffer, offset);
					length = (int)(proto & 0xFFFFFFFFFFFFL);
					offset += 8;

					if (length <= 0)
					{
						// Some server versions returned zero length groups for batch/scan/query.
						// Receive again to retrieve next group.
						ReceiveNext();
						return;
					}

					inHeader = false;
					ReallocateBuffer(length);

					Log.Debug(logContext, "Draining detail");
					conn.Receive(dataBuffer, offset, length);
					return;
				}
				else 
				{
					if (!isSingle)
					{
						// The last group trailer will never be compressed.
						bool compressed = ((ulong)(proto >> 48) & 0xff) == Command.MSG_TYPE_COMPRESSED;

						if (compressed)
						{
							// Do not recover connections with compressed data because that would
							// require saving large buffers with associated state and performing decompression
							// just to drain the connection.
							throw new AerospikeException("Recovering connections with compressed multi-record data is not supported");
						}

						// Warning: The following code assumes multi-record responses always end with a separate proto
						// that only contains one header with the info3 last group bit.  This is always true for batch
						// and scan, but query does not conform.  Therefore, connection recovery for queries will
						// likely fail.
						byte info3 = dataBuffer[offset + 3];

						if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
						{
							Log.Debug(logContext, "IsSingle false async passed draining");
							Recover();
							return;
						}

						offset += length;
						ReceiveNext();
						return;
					}
					else if (inAuth)
					{
						if (dataBuffer[offset + 1] != 0)
						{
							// Authentication failed.
							Log.Debug(logContext, "Invalid user/password");
							Abort();
							return;
						}

						Recover();
						return;
					}
					else
					{
						Recover();
						return;
					}
				}
			}
			catch (Exception e)
			{
				Log.Debug(logContext, "Error recovering async connection: ");
				Log.Debug(logContext, e.Message + e.StackTrace);
				Abort();
			}
		}

		public void ReceiveNext()
		{
			inHeader = true;
			Log.Debug(logContext, "Draining header");
			ReallocateBuffer(8);
			conn.Receive(dataBuffer, offset, 8);
		}

		private void Recover()
		{
			try
			{
				done = true;
				Log.Debug(logContext, "Async connection recovered");
				conn.UpdateLastUsed();
				node.PutAsyncConnection(conn);
				node.IncrAsyncConnsRecovered();
			}
			catch (Exception e)
			{
				Log.Debug(logContext, "AsyncConnectionRecover failed: ");
				Log.Debug(logContext, e.Message + e.StackTrace);
			}
		}

		public void ReallocateBuffer(int length)
		{
			if (dataBuffer.Length < length)
			{
				dataBuffer = new byte[length];
			}
		}

		public void OnConnected()
		{
			// This code should never get hit
			throw new Exception("OnConnected Not supported");
		}

		public void SendComplete()
		{
			// This will only get called if the original timeout was on sending a command.
			// Try to read header.
			Log.Debug(logContext, "Draining header");
			ReceiveNext();
		}

		public void OnSocketError(SocketError se)
		{
			Log.Debug(logContext, "Socket error recovering async connection: ");
			Log.Debug(logContext, se.ToString());
			Abort();
		}

		public void OnError(Exception e)
		{
			Log.Debug(logContext, "Error recovering async connection: ");
			Log.Debug(logContext, e.Message + e.StackTrace);
			Abort();
		}
	}
}
