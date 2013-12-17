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
			int failedNodes = 0;
			int failedConns = 0;
			int iterations = 0;

			dataBuffer = ThreadLocalData.GetBuffer();

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				Node node = null;
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
						node.RestoreHealth();

						// Put connection back in pool.
						node.PutConnection(conn);

						// Command has completed successfully.  Exit method.
						return;
					}
					catch (AerospikeException ae)
					{
						// Close socket to flush out possible garbage.  Do not put back in pool.
						conn.Close();
						throw ae;
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
						// IO error means connection to server node is unhealthy.
						// Reflect this status.
						node.DecreaseHealth();
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
					// Socket connection error has occurred. Decrease health and retry.
					node.DecreaseHealth();

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
			}

			throw new AerospikeException.Timeout(policy.timeout, iterations, failedNodes, failedConns);
		}

		protected internal sealed override void SizeBuffer()
		{
			if (dataOffset > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(dataOffset);
			}
		}

		protected internal void SizeBuffer(int size)
		{
			if (size > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(size);
			}
		}

		protected internal abstract Node GetNode();
		protected internal abstract void ParseResult(Connection conn);
	}
}