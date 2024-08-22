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
using System;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	public abstract class SyncCommand : Command
	{
		protected readonly Cluster cluster;
		protected readonly Policy policy;
		internal int iteration = 1;
		internal int commandSentCounter;
		internal DateTime deadline;
		protected int resultCode;
		protected int generation;
		protected int expiration;
		protected int fieldCount;
		protected int opCount;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public SyncCommand(Cluster cluster, Policy policy)
			: base(policy.socketTimeout, policy.totalTimeout, policy.maxRetries)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.deadline = DateTime.MinValue;
		}

		/// <summary>
		/// Scan/Query constructor.
		/// </summary>
		public SyncCommand(Cluster cluster, Policy policy, int socketTimeout, int totalTimeout)
			: base(socketTimeout, totalTimeout, 0)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.deadline = DateTime.MinValue;
		}

		public virtual void Execute()
		{
			if (totalTimeout > 0)
			{
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
			}
			ExecuteCommand();
		}

		public void ExecuteCommand()
		{
			Node node;
			AerospikeException exception = null;
			ValueStopwatch metricsWatch = new();
			LatencyType latencyType = cluster.MetricsEnabled ? GetLatencyType() : LatencyType.NONE;
			bool isClientTimeout;

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				try
				{
					node = GetNode();
				}
				catch (AerospikeException ae)
				{
					ae.Policy = policy;
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
					Connection conn = node.GetConnection(socketTimeout, policy.TimeoutDelay);

					try
					{
						// Set command buffer.
						WriteBuffer();

						// Send command.
						conn.Write(dataBuffer, dataOffset);
						commandSentCounter++;

						// Parse results.
						ParseResult(conn);

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
							exception = new AerospikeException.Timeout(policy, false);
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
					catch (Connection.ReadTimeout crt)
					{
						if (policy.TimeoutDelay > 0)
						{
							cluster.RecoverConnection(new ConnectionRecover(conn, node, policy.TimeoutDelay, crt, IsSingle()));
							conn = null;
						}
						else
						{
							node.CloseConnection(conn);
						}
						exception = new AerospikeException.Timeout(policy, true);
						isClientTimeout = true;
						node.AddTimeout();
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
				catch (Connection.ReadTimeout)
				{
					// Connection already handled.
					exception = new AerospikeException.Timeout(policy, true);
					isClientTimeout = true;
					node.AddTimeout();
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
					ae.Policy = policy;
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
				if (iteration > maxRetries)
				{
					break;
				}

				if (totalTimeout > 0)
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

				cluster.AddRetry();
			}

			// Retries have been exhausted.  Throw last exception.
			if (isClientTimeout)
			{
				exception = new AerospikeException.Timeout(policy, true);
			}
			exception.Node = node;
			exception.Policy = policy;
			exception.Iteration = iteration;
			exception.SetInDoubt(IsWrite(), commandSentCounter);
			throw exception;
		}

		protected internal sealed override int SizeBuffer()
		{
			dataBuffer = ThreadLocalData.GetBuffer();

			if (dataOffset > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(dataOffset);
			}
			dataOffset = 0;
			return dataBuffer.Length;
		}

		protected internal void SizeBuffer(int size)
		{
			if (size > dataBuffer.Length)
			{
				dataBuffer = ThreadLocalData.ResizeBuffer(size);
			}
		}

		protected void SkipFields(int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldlen;
			}
		}

		protected internal sealed override void End()
		{
			// Write total size of message.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		protected void ParseHeader(IConnection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, 8, Command.STATE_READ_HEADER);

			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			SizeBuffer(receiveSize);
			conn.ReadFully(dataBuffer, receiveSize, Command.STATE_READ_DETAIL);
			conn.UpdateLastUsed();

			ulong type = (ulong)(sz >> 48) & 0xff;

			if (type == Command.AS_MSG_TYPE)
			{
				dataOffset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED)
			{
				int usize = (int)ByteUtil.BytesToLong(dataBuffer, 0);
				byte[] ubuf = new byte[usize];

				ByteUtil.Decompress(dataBuffer, 8, receiveSize, ubuf, usize);
				dataBuffer = ubuf;
				dataOffset = 13;
			}
			else
			{
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			this.resultCode = dataBuffer[dataOffset] & 0xFF;
			dataOffset++;
			this.generation = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			this.expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 8;
			this.fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			this.opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
		}

		protected void ParseFields(Txn tran, Key key, bool hasWrite)
		{
			if (tran == null)
			{
				SkipFields(fieldCount);
				return;
			}

			long? version = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.RECORD_VERSION)
				{
					if (size == 7)
					{
						version = ByteUtil.VersionBytesToLong(dataBuffer, dataOffset);
					}
					else
					{
						throw new AerospikeException("Record version field has invalid size: " + size);
					}
				}
				dataOffset += size;
			}

			if (hasWrite)
			{
				tran.OnWrite(key, version, resultCode);
			}
			else
			{
				tran.OnRead(key, version);
			}
		}

		protected void ParseTranDeadline(Txn txn)
		{
			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int type = dataBuffer[dataOffset++];
				int size = len - 1;

				if (type == FieldType.MRT_DEADLINE)
				{
					int deadline = ByteUtil.LittleBytesToInt(dataBuffer, dataOffset);
					txn.Deadline = deadline;
				}
				dataOffset += size;
			}
		}

		protected internal sealed override void SetLength(int length)
		{
			dataOffset = length;
		}

		// Do nothing by default. Write commands will override this method.
		protected internal virtual void OnInDoubt()
		{

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

		protected internal virtual bool IsWrite()
		{
			return false;
		}

		protected virtual bool IsSingle()
		{
			return true;
		}

		protected internal abstract Node GetNode();

		protected abstract LatencyType GetLatencyType();
		protected internal abstract void WriteBuffer();
		protected internal abstract void ParseResult(IConnection conn);
		protected internal abstract bool PrepareRetry(bool timeout);
	}
}
