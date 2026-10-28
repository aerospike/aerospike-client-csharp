/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
		internal string ns;
		private CommandMetricsHandle commandMetrics;
		private MetricsRpcAttempt commandMetricsAttempt;
		private Node commandMetricsNode;
		private bool commandMetricsStarted;
		private bool commandMetricsClientTimeout;
		private bool queryServerMetrics;
		private QueryMetricsHandle queryMetrics;
		private readonly bool collectCommandMetrics;
		private MetricsCallContext apiMetricsContext;
		private IReadOnlyList<string> metricsNamespaces;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public SyncCommand(Cluster cluster, Policy policy, string ns)
			: this(cluster, policy, ns, true)
		{
		}

		private protected SyncCommand(
			Cluster cluster,
			Policy policy,
			string ns,
			bool collectCommandMetrics)
			: base(policy.socketTimeout, policy.totalTimeout, policy.maxRetries)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.deadline = DateTime.MinValue;
			this.ns = ns;
			this.collectCommandMetrics = collectCommandMetrics;
			apiMetricsContext = cluster.CaptureApiMetricsContext();
		}

		/// <summary>
		/// Scan/Query constructor.
		/// </summary>
		public SyncCommand(Cluster cluster, Policy policy, int socketTimeout, int totalTimeout, string ns)
			: base(socketTimeout, totalTimeout, 0)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.deadline = DateTime.MinValue;
			this.ns = ns;
			this.collectCommandMetrics = true;
			apiMetricsContext = cluster.CaptureApiMetricsContext();
		}

		internal void SetApiMetricsContext(MetricsCallContext context)
		{
			apiMetricsContext = context;
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
			StartCommandMetrics();

			try
			{
				ExecuteCommandCore();
				apiMetricsContext.MarkSuccessfulWork();
			}
			catch (AerospikeException)
			{
				apiMetricsContext.CompleteFailure();
				throw;
			}
			catch (Exception)
			{
				apiMetricsContext.CompleteFailure();
				throw;
			}
			finally
			{
				CompleteQueryServerMetrics();
			}
		}

		private void ExecuteCommandCore()
		{
			Node node;
			AerospikeException exception = null;
			bool metricsEnabled = cluster.MetricsOperationalEnabled;
			LatencyType latencyType = metricsEnabled ? GetLatencyType() : LatencyType.NONE;
			bool isClientTimeout;

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				commandMetricsClientTimeout = false;
				commandMetricsAttempt = null;

				try
				{
					node = GetNode();
					commandMetricsNode = node;
					commandMetricsAttempt = commandMetrics.BeginAttempt();
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
					Connection conn = node.GetConnection(
						socketTimeout,
						policy.TimeoutDelay,
						ns);

					try
					{
						// Set command buffer.
						WriteBuffer();

						// Send command.
						conn.Write(dataBuffer, dataOffset);
						commandSentCounter++;

						commandMetricsAttempt?.AddBytes(0, dataOffset);

						// Parse results.
						ParseResult(node, conn);
						RecordResultMetrics(node);

						// Put connection back in pool.
						node.PutConnection(conn);

						commandMetricsAttempt?.TrySucceed(node);

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
							commandMetricsClientTimeout = false;
							node.IncrErrorRate();
						}
						else if (ae.Result == ResultCode.DEVICE_OVERLOAD)
						{
							// Add to circuit breaker error count and retry.
							exception = ae;
							isClientTimeout = false;
							commandMetricsClientTimeout = false;
							node.IncrErrorRate();
						}
						else if (ae.Result == ResultCode.KEY_BUSY)
						{
							exception = ae;
							isClientTimeout = false;
							commandMetricsClientTimeout = false;
							node.IncrErrorRate();
						}
						else
						{
							commandMetricsAttempt?.TryFail(node, ae, false);
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
							cluster.RecordImmediateTimeout();
							node.CloseConnection(conn);
						}
						exception = new AerospikeException.Timeout(policy, true);
						isClientTimeout = true;
						commandMetricsClientTimeout = true;
					}
					catch (SocketException se)
					{
						// Socket errors are considered temporary anomalies.
						// Retry after closing connection.
						node.CloseConnectionOnError(conn);

						if (se.SocketErrorCode == SocketError.TimedOut)
						{
							exception = new AerospikeException.Timeout(policy, true);
							isClientTimeout = true;
							commandMetricsClientTimeout = true;
						}
						else
						{
							exception = new AerospikeException.Connection(se);
							isClientTimeout = false;
							commandMetricsClientTimeout = false;
						}
					}
					catch (IOException ioe)
					{
						// IO errors are considered temporary anomalies.  Retry.
						// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						node.CloseConnection(conn);
						exception = new AerospikeException.Connection(ioe);
						isClientTimeout = false;
						commandMetricsClientTimeout = false;
					}
					catch (Exception)
					{
						// All other exceptions are considered fatal.  Do not retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.CloseConnectionOnError(conn);
						commandMetricsAttempt?.TryFail(
							node,
							new AerospikeException("Command failed"),
							false);
						throw;
					}
				}
				catch (SocketException se)
				{
					// This exception might happen after initial connection succeeded, but
					// user login failed with a socket error.  Retry.
					if (se.SocketErrorCode == SocketError.TimedOut)
					{
						exception = new AerospikeException.Timeout(policy, true);
						isClientTimeout = true;
						commandMetricsClientTimeout = true;
					}
					else
					{
						exception = new AerospikeException.Connection(se);
						isClientTimeout = false;
						commandMetricsClientTimeout = false;
					}
				}
				catch (IOException ioe)
				{
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
					commandMetricsClientTimeout = false;
				}
				catch (Connection.ReadTimeout)
				{
					// Connection already handled.
					exception = new AerospikeException.Timeout(policy, true);
					isClientTimeout = true;
					commandMetricsClientTimeout = true;
				}
				catch (AerospikeException.Connection ce)
				{
					// Socket connection error has occurred. Retry.
					exception = ce;
					isClientTimeout = false;
					commandMetricsClientTimeout = false;
				}
				catch (AerospikeException.Backoff be)
				{
					// Node is in backoff State. Retry, hopefully on another node.
					exception = be;
					isClientTimeout = false;
					commandMetricsClientTimeout = false;
				}
				catch (AerospikeException ae)
				{
					ae.Node = node;
					ae.Policy = policy;
					ae.Iteration = iteration;
					ae.SetInDoubt(IsWrite(), commandSentCounter);
					commandMetricsAttempt?.TryFail(node, ae, false);
					throw;
				}
				catch (Exception e)
				{
					commandMetricsAttempt?.TryFail(
						node,
						new AerospikeException(e),
						false);
					throw;
				}

				commandMetricsAttempt?.TryFail(
					node,
					exception,
					commandMetricsClientTimeout);

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
				commandMetrics.AddRetry();
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

		private protected void StartCommandMetrics()
		{
			if (commandMetricsStarted)
			{
				return;
			}
			commandMetricsStarted = true;

			if (!collectCommandMetrics && !apiMetricsContext.CompatibilityOnly)
			{
				return;
			}

			LatencyType type = GetLatencyType();

			if (type == LatencyType.NONE)
			{
				return;
			}

			bool countCommand = type == LatencyType.READ || type == LatencyType.WRITE;
			commandMetrics = cluster.BeginMetricsCommand(
				apiMetricsContext,
				ns,
				metricsNamespaces,
				type,
				countCommand);

			if (type == LatencyType.QUERY)
			{
				queryMetrics = cluster.BeginQueryMetrics(apiMetricsContext);

				if (queryMetrics.Enabled)
				{
					queryMetrics.AddServer(1);
					queryServerMetrics = true;
				}
			}
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

		protected internal sealed override void End()
		{
			// Write total size of message.
			ulong size = ((ulong)dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		protected internal void ParseHeader(Node node, Connection conn)
		{
			int bytesIn = 0;

			// Read header.
			try
			{
				conn.ReadFully(dataBuffer, 8, Command.STATE_READ_HEADER);
			}
			catch (Connection.ReadTimeout timeout)
			{
				AddCommandMetricsBytes(node, timeout.offset, 0);
				throw;
			}
			bytesIn += 8;

			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			SizeBuffer(receiveSize);
			try
			{
				conn.ReadFully(dataBuffer, receiveSize, Command.STATE_READ_DETAIL);
			}
			catch (Connection.ReadTimeout timeout)
			{
				AddCommandMetricsBytes(node, bytesIn + timeout.offset, 0);
				throw;
			}
			bytesIn += receiveSize;
			AddCommandMetricsBytes(node, bytesIn, 0);
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

		private protected void AddCommandMetricsBytes(Node node, long bytesIn, long bytesOut)
		{
			commandMetricsAttempt?.AddBytes(bytesIn, bytesOut);
		}

		private protected CommandMetricsHandle ForkCommandMetrics(
			string childNamespace,
			IReadOnlyList<string> childNamespaces = null)
		{
			return commandMetrics.ForkRpc(
				childNamespace,
				childNamespaces);
		}

		private protected void SetMetricsNamespaces(
			IReadOnlyList<string> namespaces)
		{
			metricsNamespaces = namespaces;
		}

		private protected void RecordBatchRecordFailure(
			Node node,
			string recordNamespace,
			int recordResultCode)
		{
			commandMetricsAttempt?.RecordBatchRecordFailure(
				node,
				recordNamespace,
				recordResultCode);
		}

		private protected virtual void RecordResultMetrics(Node node)
		{
		}

		private protected void SetCommandMetrics(CommandMetricsHandle handle)
		{
			commandMetrics = handle;
			commandMetricsStarted = true;
		}

		private protected void DelegateCommandMetrics()
		{
			CompleteQueryServerMetrics();
		}

		private protected void AddCommandMetricsRetry()
		{
			commandMetrics.AddRetry();
		}

		private void CompleteQueryServerMetrics()
		{
			if (queryServerMetrics)
			{
				queryServerMetrics = false;
				queryMetrics.AddServer(-1);
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
		protected internal abstract void ParseResult(Node node, Connection conn);
		protected internal abstract bool PrepareRetry(bool timeout);
	}
}
