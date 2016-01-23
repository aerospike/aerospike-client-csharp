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
using System.Net.Sockets;
using System.Threading;

namespace Aerospike.Client
{
	public abstract class SyncCommand : Command
	{
		public void Execute()
		{
			Policy policy = GetPolicy();
			int remainingMillis = policy.timeout;
			DateTime limit = DateTime.UtcNow.AddMilliseconds(remainingMillis);
			Node node = null;
			int failedNodes = 0;
			int failedConns = 0;
			int iterations = 0;

			dataBuffer = ThreadLocalData.GetBuffer();

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				try
				{
					node = GetNode();
					Connection conn = node.GetConnection(remainingMillis);

					try
					{
						// Set command buffer.
						WriteBuffer();

						// Reset timeout in send buffer (destined for server) and socket.
						ByteUtil.IntToBytes((uint)remainingMillis, dataBuffer, 22);

						// Send command.
						conn.Write(dataBuffer, dataOffset);

						// Parse results.
						ParseResult(conn);

						// Reflect healthy status.
						conn.UpdateLastUsed();

						// Put connection back in pool.
						node.PutConnection(conn);

						// Command has completed successfully.  Exit method.
						return;
					}
					catch (AerospikeException ae)
					{
						if (ae.KeepConnection())
						{
							// Put connection back in pool.
							conn.UpdateLastUsed();
							node.PutConnection(conn);
						}
						else
						{
							// Close socket to flush out possible garbage.  Do not put back in pool.
							conn.Close();
						}
						throw;
					}
					catch (SocketException ioe)
					{
						// IO errors are considered temporary anomalies.  Retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						conn.Close();

						if (Log.DebugEnabled())
						{
							Log.Debug("Node " + node + ": " + Util.GetErrorMessage(ioe));
						}
					}
					catch (Exception)
					{
						// All runtime exceptions are considered fatal.  Do not retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						conn.Close();
						throw;
					}
				}
				catch (AerospikeException.InvalidNode)
				{
					// Node is currently inactive.  Retry.
					failedNodes++;
				}
				catch (AerospikeException.Connection ce)
				{
					// Socket connection error has occurred. Retry.
					if (Log.DebugEnabled())
					{
						Log.Debug("Node " + node + ": " + Util.GetErrorMessage(ce));
					}
					failedConns++;
				}

				if (++iterations > policy.maxRetries)
				{
					break;
				}

				// Check for client timeout.
				if (policy.timeout > 0)
				{
					remainingMillis = (int)limit.Subtract(DateTime.UtcNow).TotalMilliseconds - policy.sleepBetweenRetries;

					if (remainingMillis <= 0)
					{
						break;
					}
				}

				if (policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(policy.sleepBetweenRetries);
				}

				// Reset node reference and try again.
				node = null;
			}

			throw new AerospikeException.Timeout(node, policy.timeout, iterations, failedNodes, failedConns);
		}

		protected internal sealed override void SizeBuffer()
		{
			if (dataOffset > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(dataOffset);
			}
			dataOffset = 0;
		}

		protected internal void SizeBuffer(int size)
		{
			if (size > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(size);
			}
		}

		protected internal sealed override void End()
		{
			// Write total size of message.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		protected internal void EmptySocket(Connection conn)
		{
			// There should not be any more bytes.
			// Empty the socket to be safe.
			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int headerLength = dataBuffer[8];
			int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

			// Read remaining message bytes.
			if (receiveSize > 0)
			{
				SizeBuffer(receiveSize);
				conn.ReadFully(dataBuffer, receiveSize);
			}
		}
	
		protected internal abstract Node GetNode();
		protected internal abstract void ParseResult(Connection conn);
	}
}
