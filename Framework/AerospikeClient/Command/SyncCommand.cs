/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
		public void Execute(Cluster cluster, Policy policy, bool isRead)
		{
			DateTime deadline = DateTime.MinValue;
			int socketTimeout = policy.socketTimeout;
			int totalTimeout = policy.totalTimeout;

			if (totalTimeout > 0)
			{
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);

				if (socketTimeout == 0 || socketTimeout > totalTimeout)
				{
					socketTimeout = totalTimeout;
				}
			}
			Execute(cluster, policy, isRead, socketTimeout, totalTimeout, deadline, 1, 0);
		}

		public void Execute
		(
			Cluster cluster,
			Policy policy,
			bool isRead,
			int socketTimeout,
			int totalTimeout,
			DateTime deadline,
			int iteration,
			int commandSentCounter
		)
		{
			Node node;
			AerospikeException exception = null;
			bool isClientTimeout;

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				try
				{
					node = GetNode(cluster);
				}
				catch (AerospikeException ae)
				{
					ae.Policy = policy;
					ae.Iteration = iteration;
					ae.SetInDoubt(isRead, commandSentCounter);
					throw;
				}

				try
				{
					Connection conn = node.GetConnection(socketTimeout);

					try
					{
						// Set command buffer.
						WriteBuffer();

						// Check if timeout needs to be changed in send buffer.
						if (totalTimeout != policy.totalTimeout)
						{
							// Reset timeout in send buffer (destined for server) and socket.
							ByteUtil.IntToBytes((uint)totalTimeout, dataBuffer, 22);
						}

						// Send command.
						conn.Write(dataBuffer, dataOffset);
						commandSentCounter++;

						// Parse results.
						ParseResult(conn);

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
							node.PutConnection(conn);
						}
						else
						{
							// Close socket to flush out possible garbage.  Do not put back in pool.
							node.CloseConnection(conn);
						}

						if (ae.Result == ResultCode.TIMEOUT)
						{
							// Go through retry logic on server timeout.
							exception = new AerospikeException.Timeout(policy, false);
							isClientTimeout = false;
						}
						else
						{
							throw;
						}
					}
					catch (SocketException se)
					{
						// Socket errors are considered temporary anomalies.
						// Retry after closing connection.
						node.CloseConnection(conn);

						if (se.SocketErrorCode == SocketError.TimedOut)
						{
							isClientTimeout = true;
						}
						else
						{
							exception = new AerospikeException(se);
							isClientTimeout = false;
						}
					}
					catch (Exception)
					{
						// All other exceptions are considered fatal.  Do not retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.CloseConnection(conn);
						throw;
					}
				}
				catch (SocketException se)
				{
					// This exception might happen after initial connection succeeded, but
					// user login failed with a socket error.  Retry.
					if (se.SocketErrorCode == SocketError.TimedOut)
					{
						isClientTimeout = true;
					}
					else
					{
						exception = new AerospikeException(se);
						isClientTimeout = false;
					}
				}
				catch (AerospikeException.Connection ce)
				{
					// Socket connection error has occurred. Retry.
					exception = ce;
					isClientTimeout = false;
				}
				catch (AerospikeException ae)
				{
					ae.Node = node;
					ae.Policy = policy;
					ae.Iteration = iteration;
					ae.SetInDoubt(isRead, commandSentCounter);
					throw;
				}

				// Check maxRetries.
				if (iteration > policy.maxRetries)
				{
					break;
				}

				if (policy.totalTimeout > 0)
				{
					// Check for total timeout.
					long remaining = (long)deadline.Subtract(DateTime.UtcNow).TotalMilliseconds - policy.sleepBetweenRetries;

					if (remaining <= 0)
					{
						break;
					}

					if (remaining < totalTimeout)
					{
						totalTimeout = (int)remaining;

						if (socketTimeout > totalTimeout)
						{
							socketTimeout = totalTimeout;
						}
					}
				}

				if (!isClientTimeout && policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(policy.sleepBetweenRetries);
				}

				iteration++;

				if (!PrepareRetry(isClientTimeout || exception.Result != ResultCode.SERVER_NOT_AVAILABLE))
				{
					// Batch may be retried in separate commands.
					if (RetryBatch(cluster, socketTimeout, totalTimeout, deadline, iteration, commandSentCounter))
					{
						// Batch was retried in separate commands.  Complete this command.
						return;
					}
				}
			}

			// Retries have been exhausted.  Throw last exception.
			if (isClientTimeout)
			{
				exception = new AerospikeException.Timeout(policy, true);
			}
			exception.Node = node;
			exception.Policy = policy;
			exception.Iteration = iteration;
			exception.SetInDoubt(isRead, commandSentCounter);
			throw exception;
		}

		protected internal sealed override void SizeBuffer()
		{
			dataBuffer = ThreadLocalData.GetBuffer();

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

		protected internal virtual bool RetryBatch
		(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			DateTime deadline,
			int iteration,
			int commandSentCounter
		)
		{
			// Override this method in batch to regenerate node assignments.
			return false;
		}

		protected internal abstract Node GetNode(Cluster cluster);
		protected internal abstract void WriteBuffer();
		protected internal abstract void ParseResult(Connection conn);
		protected internal abstract bool PrepareRetry(bool timeout);
	}
}
