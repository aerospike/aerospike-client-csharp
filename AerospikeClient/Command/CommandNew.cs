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
using Microsoft.VisualBasic;
using Neo.IronLua;
using System.Buffers;
using System.Collections;
using System.Diagnostics.Metrics;
using System.Drawing;
using System.Net.Sockets;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	public abstract class CommandNew
	{
		internal int ServerTimeout { get; private set; }
		internal int SocketTimeout { get; private set; }
		internal int TotalTimeout { get; private set; }
		internal int MaxRetries { get; private set; }

		protected Cluster Cluster { get; private set; }
		protected Policy Policy { get; private set; }

		internal byte[] dataBuffer;
		internal int dataOffset;
		internal int iteration = 1;
		internal int commandSentCounter;
		internal DateTime deadline;

		public ArrayPool<byte> BufferPool { get; }


		public CommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Policy policy)
		{
			this.Cluster = cluster;
			this.Policy = policy;
			this.MaxRetries = policy.maxRetries;
			this.TotalTimeout = policy.totalTimeout;

			if (TotalTimeout > 0)
			{
				this.SocketTimeout = (policy.socketTimeout < TotalTimeout && policy.socketTimeout > 0) ? policy.socketTimeout : TotalTimeout;
				this.ServerTimeout = this.SocketTimeout;
			}
			else
			{
				this.SocketTimeout = policy.socketTimeout;
				this.ServerTimeout = 0;
			}

			this.BufferPool = bufferPool;
		}

		public async Task Execute(CancellationToken token)
		{
			if (TotalTimeout > 0)
			{
				deadline = DateTime.UtcNow.AddMilliseconds(TotalTimeout);
			}
			await ExecuteCommand(token);
		}

		private async Task ExecuteCommand(CancellationToken token)
		{
			Node node;
			AerospikeException exception = null;
			ValueStopwatch metricsWatch = new();
			LatencyType latencyType = Cluster.MetricsEnabled ? GetLatencyType() : LatencyType.NONE;
			bool isClientTimeout;

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				token.ThrowIfCancellationRequested();

				try
				{
					node = GetNode();
				}
				catch (AerospikeException ae)
				{
					ae.Policy = Policy;
					ae.Iteration = iteration;
					ae.SetInDoubt(IsWrite(), commandSentCounter);
					throw;
				}

				try
				{
					node.ValidateErrorCount();
					if (latencyType != LatencyType.NONE)
					{
						metricsWatch = ValueStopwatch.StartNew();
					}
					//Connection conn = await node.GetConnection(SocketTimeout, token);
					Connection conn = node.GetConnection(SocketTimeout);

					try
					{
						// Set command buffer.
						WriteBuffer();

						// Send command.
						//conn.Write(dataBuffer, dataOffset);
						await conn.Write(dataBuffer, dataOffset, token);
						commandSentCounter++;

						// Parse results.
						await ParseResult(conn, token);
						//ParseResult(conn);

						// Put connection back in pool.
						node.PutConnection(conn);

						if (latencyType != LatencyType.NONE)
						{
							node.AddLatency(latencyType, metricsWatch.Elapsed.TotalMilliseconds);
						}

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
							node.CloseConnectionOnError(conn);
						}

						if (ae.Result == ResultCode.TIMEOUT)
						{
							// Retry on server timeout.
							exception = new AerospikeException.Timeout(Policy, false);
							isClientTimeout = false;
							node.IncrErrorRate();
							node.AddTimeout();
						}
						else if (ae.Result == ResultCode.DEVICE_OVERLOAD)
						{
							// Add to circuit breaker error count and retry.
							exception = ae;
							isClientTimeout = false;
							node.IncrErrorRate();
							node.AddError();
						}
						else
						{
							node.AddError();
							throw;
						}
					}
					catch (SocketException se)
					{
						// Socket errors are considered temporary anomalies.
						// Retry after closing connection.
						node.CloseConnectionOnError(conn);

						if (se.SocketErrorCode == SocketError.TimedOut)
						{
							isClientTimeout = true;
							node.AddTimeout();
						}
						else
						{
							exception = new AerospikeException.Connection(se);
							isClientTimeout = false;
							node.AddError();
						}
					}
					catch (IOException ioe)
					{
						// IO errors are considered temporary anomalies.  Retry.
						// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						node.CloseConnection(conn);
						exception = new AerospikeException.Connection(ioe);
						isClientTimeout = false;
						node.AddError();
					}
					catch (Exception)
					{
						// All other exceptions are considered fatal.  Do not retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.CloseConnectionOnError(conn);
						node.AddError();
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
						node.AddTimeout();
					}
					else
					{
						exception = new AerospikeException.Connection(se);
						isClientTimeout = false;
						node.AddError();
					}
				}
				catch (IOException ioe)
				{
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException.Connection ce)
				{
					// Socket connection error has occurred. Retry.
					exception = ce;
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException.Backoff be)
				{
					// Node is in backoff state. Retry, hopefully on another node.
					exception = be;
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException ae)
				{
					ae.Node = node;
					ae.Policy = Policy;
					ae.Iteration = iteration;
					ae.SetInDoubt(IsWrite(), commandSentCounter);
					node.AddError();
					throw;
				}
				catch (Exception)
				{
					node.AddError();
					throw;
				}

				// Check maxRetries.
				if (iteration > MaxRetries)
				{
					break;
				}

				if (TotalTimeout > 0)
				{
					// Check for total timeout.
					long remaining = (long)deadline.Subtract(DateTime.UtcNow).TotalMilliseconds - Policy.sleepBetweenRetries;

					if (remaining <= 0)
					{
						break;
					}

					if (remaining < TotalTimeout)
					{
						TotalTimeout = (int)remaining;

						if (SocketTimeout > TotalTimeout)
						{
							SocketTimeout = TotalTimeout;
						}
					}
				}

				if (!isClientTimeout && Policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(Policy.sleepBetweenRetries);
				}

				iteration++;

				if (!PrepareRetry(isClientTimeout || exception.Result != ResultCode.SERVER_NOT_AVAILABLE))
				{
					// Batch may be retried in separate commands.
					if (RetryBatch(Cluster, SocketTimeout, TotalTimeout, deadline, iteration, commandSentCounter))
					{
						// Batch was retried in separate commands.  Complete this command.
						return;
					}
				}

				Cluster.AddRetry();
			}

			// Retries have been exhausted.  Throw last exception.
			if (isClientTimeout)
			{
				exception = new AerospikeException.Timeout(Policy, true);
			}
			exception.Node = node;
			exception.Policy = Policy;
			exception.Iteration = iteration;
			exception.SetInDoubt(IsWrite(), commandSentCounter);
			throw exception;
		}

		public virtual bool RetryBatch
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

		internal abstract bool IsWrite();
		internal abstract Node GetNode();

		internal abstract LatencyType GetLatencyType();

		public abstract void WriteBuffer();
		public abstract Task ParseResult(IConnection conn, CancellationToken token);
		public abstract bool PrepareRetry(bool timeout);

		
	}
}
