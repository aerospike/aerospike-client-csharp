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
using System;
using System.Net.Sockets;
using System.Runtime.ConstrainedExecution;
using System.Timers;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous command handler.
	/// </summary>
	public abstract class AsyncCommand : Command, IAsyncCommand, ITimeout
	{
		private static int ErrorCount = 0;
		private const int IN_PROGRESS = 0;
		private const int SUCCESS = 1;
		private const int RETRY = 2;
		private const int FAIL_TOTAL_TIMEOUT = 3;
		private const int FAIL_NETWORK_INIT = 4;
		private const int FAIL_NETWORK_ERROR = 5;
		private const int FAIL_APPLICATION_INIT = 6;
		private const int FAIL_APPLICATION_ERROR = 7;
		private const int FAIL_SOCKET_TIMEOUT = 8;
		private const int FAIL_QUEUE_ERROR = 9;

		protected internal readonly AsyncCluster cluster;
		protected internal Policy policy;
		private AsyncConnection conn;
		protected internal AsyncNode node;
		private BufferSegment segmentOrig;
		private BufferSegment segment;
		private ValueStopwatch socketWatch;
		private ValueStopwatch totalWatch;
		protected internal int dataLength;
		private int iteration;
		protected internal int commandSentCounter;
		private volatile int state;
		private volatile bool eventReceived;
		private bool compressed;
		private bool inAuthenticate;
		private bool inHeader = true;
		private ValueStopwatch metricsWatch;
		private readonly bool metricsEnabled;
		private readonly string ns;
		private long bytesIn;
		private long bytesOut;

		/// <summary>
		/// Default Constructor.
		/// </summary>
		public AsyncCommand(AsyncCluster cluster, Policy policy, string ns)
			: base(policy.socketTimeout, policy.totalTimeout, policy.maxRetries)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.metricsEnabled = cluster.MetricsEnabled;
			this.ns = ns;
		}

		/// <summary>
		/// Scan/Query Constructor.
		/// </summary>
		public AsyncCommand(AsyncCluster cluster, Policy policy, int socketTimeout, int totalTimeout, string ns)
			: base(socketTimeout, totalTimeout, 0)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.metricsEnabled = cluster.MetricsEnabled;
			this.ns = ns;
		}

		/// <summary>
		/// Clone constructor.
		/// </summary>
		public AsyncCommand(AsyncCommand other)
			: base(other.socketTimeout, other.totalTimeout, other.maxRetries)
		{
			// Retry constructor.
			this.cluster = other.cluster;
			this.policy = other.policy;
			this.segmentOrig = other.segmentOrig;
			this.segment = other.segment;
			this.totalWatch = other.totalWatch;
			this.iteration = other.iteration;
			this.commandSentCounter = other.commandSentCounter;
			this.metricsEnabled = cluster.MetricsEnabled;
		}

		public void SetBatchRetry(AsyncCommand other)
		{
			// This batch retry command will be added to the timeout queue in ExecuteCore().
			this.iteration = other.iteration;
			this.totalWatch = other.totalWatch;
		}

        // Simply ask the cluster object to schedule the command for execution.
        // It may or may not be immediate, depending on the number of executing commands.
        // If immediate, the command will start its execution on the current thread.
        // Otherwise, the command will start its execution from the thread pool.
        public void Execute()
        {
			if (totalTimeout > 0)
			{
				// totalTimeout is a fixed timeout. Stopwatch is started once on first
				// attempt and not restarted on retry.
				totalWatch = ValueStopwatch.StartNew();
				AsyncTimeoutQueue.Instance.Add(this, totalTimeout);
			}
			cluster.ScheduleCommandExecution(this);
        }

		public void ExecuteBatchRetry()
		{
			if (totalTimeout > 0)
			{
				int remain = (int)(totalTimeout - totalWatch.ElapsedMilliseconds);
				AsyncTimeoutQueue.Instance.Add(this, remain);
			}
			cluster.ScheduleCommandExecution(this);
		}

		// Executes the command on the current thread.
		internal void ExecuteInline(BufferSegment segment)
		{
			// Use global consective error count to determine if it's safe to run command immediately.
			// ErrorCount does not need to be atomic because it's just a general indicator that stack
			// recursion may get out of control.  Absolute accuracy is not required in this case.
			if (ErrorCount < 5)
			{
				// Execute command now.
				this.segment = this.segmentOrig = segment;
				ExecuteCore();
			}
			else
			{
				// Prevent recursive error stack overflow by placing command in a queue.
				ExecuteAsync(segment);
			}
		}

		// Executes the command from the thread pool.
		internal void ExecuteAsync(BufferSegment segment)
		{
			this.segment = this.segmentOrig = segment;
			ThreadPool.UnsafeQueueUserWorkItem(EventHandler.ExecuteAsyncHandler, this);
		}

		// Use a singleton event handler to avoid creating a new delegate for each command.
		private sealed class EventHandler
		{
			private static readonly EventHandler Instance = new EventHandler();
			public static readonly WaitCallback ExecuteAsyncHandler = Instance.HandleExecution;

			private void HandleExecution(object state)
			{
				AsyncCommand cmd = state as AsyncCommand;

				try
				{
					cmd.ExecuteCore();
				}
				catch (Exception e)
				{
					Log.Error(cmd.cluster.context, "ExecuteCore error: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void ExecuteCore()
		{
			if (totalTimeout > 0)
			{
				// Timeout already added in Execute(). Verify State.
				if (state != IN_PROGRESS)
				{
					// Total timeout might have occurred if command was in the delay queue.
					// Socket timeout should not be possible for commands in the delay queue.
					if (state != FAIL_TOTAL_TIMEOUT)
					{
						Log.Error(cluster.context, "Unexpected State at async command start: " + state);
					}
					// User has already been notified of the total timeout. Release buffer and 
					// return for all error states.
					ReleaseBuffer();
					return;
				}

				if (socketTimeout > 0)
				{
					// socketTimeout is an idle timeout. socketWatch is restarted on every attempt.
					socketWatch = ValueStopwatch.StartNew();
				}
			}
			else if (socketTimeout > 0)
			{
				socketWatch = ValueStopwatch.StartNew();
				AsyncTimeoutQueue.Instance.Add(this, socketTimeout);
			}
			ExecuteCommand();
		}

		private void ExecuteCommand()
		{
			iteration++;
			bytesIn = 0;
			bytesOut = 0;

			try
			{
				node = (AsyncNode)GetNode(cluster);
				node.ValidateErrorCount();

				if (metricsEnabled)
				{
					metricsWatch = ValueStopwatch.StartNew();
				}
				conn = node.GetAsyncConnection();

				if (conn == null)
				{
					node.IncrAsyncConnTotal();
					conn = node.CreateAsyncConnection(this);
					conn.Connect(node.address);
				}
				else
				{
					conn.Command = this;
					ConnectionReady();
				}
				ErrorCount = 0;
			}
			catch (AerospikeException.Connection aec)
			{
				ErrorCount++;
				node?.AddError(ns);
				ConnectionFailed(aec);
			}
			catch (AerospikeException.Backoff aeb)
			{
				ErrorCount++;
				node?.AddError(ns);
				Backoff(aeb);
			}
			catch (AerospikeException ae)
			{
				ErrorCount++;
				node?.AddError(ns);
				FailOnApplicationError(ae);
			}
			catch (SocketException se)
			{
				ErrorCount++;
				node?.AddError(ns);
				OnSocketError(se.SocketErrorCode);
			}
			catch (IOException ioe)
			{
				// IO errors are considered temporary anomalies.  Retry.
				ErrorCount++;
				node?.AddError(ns);
				ConnectionFailed(new AerospikeException.Connection(ioe));
			}
			catch (Exception e)
			{
				ErrorCount++;
				node?.AddError(ns);
				FailOnApplicationError(new AerospikeException(e));
			}
		}

		public void OnConnected()
		{
			if (metricsEnabled)
			{
				node.AddLatency(ns, LatencyType.CONN, metricsWatch.Elapsed.TotalMilliseconds);
			}
			
			if (cluster.authEnabled)
			{
				byte[] token = node.SessionToken;

				if (token != null)
				{
					inAuthenticate = true;
					// Authentication messages are small.  Set a reasonable upper bound.
					dataOffset = 200;
					SizeBuffer();

					AdminCommand command = new AdminCommand(dataBuffer, dataOffset);
					dataLength = command.SetAuthenticate(cluster, token);
					conn?.Send(dataBuffer, dataOffset, dataLength - dataOffset);
					bytesOut += dataLength - dataOffset;
					return;
				}
			}
			ConnectionReady();
		}

		private void ConnectionReady()
		{
			WriteBuffer();
			conn?.Send(dataBuffer, dataOffset, dataLength - dataOffset);
			bytesOut += dataLength - dataOffset;
		}

		protected internal sealed override int SizeBuffer()
		{
			// dataOffset is currently the estimate, which may be greater than the actual size.
			dataLength = dataOffset;

			if (dataLength > segment.size)
			{
				ResizeBuffer(dataLength);
			}
			dataBuffer = segment.buffer;
			dataOffset = segment.offset;
			return segment.size;
		}

		private void ResizeBuffer(int size)
		{
			// Large buffers should not be cached.
			// Allocate, but do not put back into pool.
			segment = new BufferSegment(-1, size);
		}

		protected internal sealed override void End()
		{
			// Write total size of message.
			int length = dataOffset - segment.offset;

			if (length > dataLength)
			{
				throw new AerospikeException("Actual buffer length " + length + " is greater than estimated length " + dataLength);
			}

			// Switch dataLength from length to buffer end offset.
			dataLength = dataOffset;
			dataOffset = segment.offset;

			ulong size = ((ulong)length - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, segment.offset);
		}

		protected internal sealed override void SetLength(int length)
		{
			dataLength = dataOffset + length;
		}

		protected internal void EndInfo()
		{
			// Write total size of message.
			int length = dataOffset - segment.offset;

			if (length > dataLength)
			{
				throw new AerospikeException("Actual buffer length " + length + " is greater than estimated length " + dataLength);
			}

			// Switch dataLength from length to buffer end offset.
			dataLength = dataOffset;
			dataOffset = segment.offset;

			ulong size = ((ulong)length - 8) | (2UL << 56) | (1UL << 48);
			ByteUtil.LongToBytes(size, dataBuffer, segment.offset);
		}

		public void SendComplete()
		{
			commandSentCounter++;

			if (socketTimeout > 0)
			{
				eventReceived = false;
			}

			conn?.Receive(dataBuffer, segment.offset, 8);
			bytesIn += 8;
		}

		public void ReceiveComplete()
		{
			if (socketTimeout > 0)
			{
				eventReceived = true;
			}

			dataOffset = segment.offset;

			if (inHeader)
			{
				long proto = ByteUtil.BytesToLong(dataBuffer, dataOffset);
				int length = (int)(proto & 0xFFFFFFFFFFFFL);

				if (length <= 0)
				{
					// Some server versions returned zero length groups for batch/scan/query.
					// Receive again to retrieve next group.
					conn?.Receive(dataBuffer, dataOffset, 8);
					bytesIn += 8;
					return;
				}

				compressed = ((proto >> 48) & 0xFF) == (long)Command.MSG_TYPE_COMPRESSED;
				inHeader = false;

				if (length > segment.size)
				{
					ResizeBuffer(length);
					dataBuffer = segment.buffer;
					dataOffset = segment.offset;
				}

				dataLength = dataOffset + length;
				conn?.Receive(dataBuffer, dataOffset, length);
				bytesIn += length - dataOffset;
			}
			else
			{
				if (inAuthenticate)
				{
					inAuthenticate = false;
					inHeader = true;

					int resultCode = dataBuffer[dataOffset + 1];

					if (resultCode != 0 && resultCode != ResultCode.SECURITY_NOT_ENABLED)
					{
						// Authentication failed. Session token probably expired.
						// Signal tend thread to perform node login, so future
						// commands do not fail.
						node.SignalLogin();

						// This is a rare event because the client tracks session
						// expiration and will relogin before session expiration.
						// Do not try to login on same socket because login can take
						// a long time and thousands of simultaneous logins could
						// overwhelm server.
						throw new AerospikeException(resultCode);
					}
					ConnectionReady();
					return;
				}

				conn?.UpdateLastUsed();

				if (compressed)
				{
					int usize = (int)ByteUtil.BytesToLong(dataBuffer, dataOffset);
					dataOffset += 8;
					byte[] ubuf = new byte[usize];

					ByteUtil.Decompress(dataBuffer, dataOffset, dataLength, ubuf, usize);
					dataBuffer = ubuf;
					dataOffset = 8;
					dataLength = usize;
				}

				ParseCommand();
			}
		}

		public void ReceiveNext()
		{
			inHeader = true;
			conn?.Receive(dataBuffer, segment.offset, 8);
			bytesIn += 8;
		}

		public void OnError(Exception e)
		{
			try
			{
				if (e is AerospikeException.Connection ac)
				{
					node.AddError(ns);
					ConnectionFailed(ac);
					return;
				}

				if (e is AerospikeException ae)
				{
					if (ae.Result == ResultCode.TIMEOUT)
					{
						node.AddTimeout(ns);
						RetryServerError(new AerospikeException.Timeout(policy, false));
					}
					else if (ae.Result == ResultCode.DEVICE_OVERLOAD)
					{
						node.AddError(ns);
						RetryServerError(ae);
					}
					else if (ae.Result == ResultCode.KEY_BUSY)
					{
						node.AddError(ns);
						node.AddKeyBusy(ns);
					}
					else
					{
						node.AddError(ns);
						FailOnApplicationError(ae);
					}
					return;
				}

				if (e is SocketException se)
				{
					OnSocketError(se.SocketErrorCode);
					return;
				}

				if (e is IOException ioe)
				{
					// IO errors are considered temporary anomalies.  Retry.
					node.AddError(ns);
					ConnectionFailed(new AerospikeException.Connection(ioe));
					return;
				}

				if (e is ObjectDisposedException ode)
				{
					// This exception occurs because socket is being used after timeout thread closes socket.
					// Retry when this happens.
					node.AddError(ns);
					ConnectionFailed(new AerospikeException(ode));
					return;
				}

				// Fail without retry on unknown errors.
				node.AddError(ns);
				FailOnApplicationError(new AerospikeException(e));
			}
			catch (Exception e2)
			{
				Log.Error(cluster.context, "OnError() failed: " + Util.GetErrorMessage(e2) +
					System.Environment.NewLine + "Original error: " + Util.GetErrorMessage(e));
			}
		}

		public void OnSocketError(SocketError se)
		{
			AerospikeException ae;

			if (se == SocketError.TimedOut)
			{
				ae = new AerospikeException.Timeout(policy, true);
				node.AddTimeout(ns);
			}
			else
			{
				ae = new AerospikeException.Connection("Socket error: " + se);
				node.AddError(ns);
			}
			ConnectionFailed(ae);
		}

		private void ConnectionFailed(AerospikeException ae)
		{
			if (ShouldRetry())
			{
				int status = Interlocked.CompareExchange(ref state, RETRY, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					CloseConnection();
					Retry(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
			else
			{
				int status = Interlocked.CompareExchange(ref state, FAIL_NETWORK_ERROR, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					CloseConnection();
					FailCommand(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
		}

		private void RetryServerError(AerospikeException ae)
		{
			node.IncrErrorRate();

			if (ShouldRetry())
			{
				int status = Interlocked.CompareExchange(ref state, RETRY, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					node.PutAsyncConnection(conn);
					Retry(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
			else
			{
				int status = Interlocked.CompareExchange(ref state, FAIL_APPLICATION_ERROR, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					node.PutAsyncConnection(conn);
					FailCommand(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
		}

		private void Backoff(AerospikeException ae)
		{
			AddBytesInOut();
			if (ShouldRetry())
			{
				int status = Interlocked.CompareExchange(ref state, RETRY, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					Retry(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
			else
			{
				int status = Interlocked.CompareExchange(ref state, FAIL_APPLICATION_ERROR, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					FailCommand(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
		}

		private bool ShouldRetry()
		{
			return iteration <= maxRetries && (totalTimeout == 0 || totalWatch.ElapsedMilliseconds < totalTimeout);
		}

		private void Retry(AerospikeException ae)
		{
			// Prepare for retry.
			if (!PrepareRetry(ae.Result != ResultCode.SERVER_NOT_AVAILABLE))
			{
				try
				{
					// Batch may be retried in separate commands.
					if (RetryBatch())
					{
						// Batch was retried in separate commands.  Complete this command.
						return;
					}
				}
				catch (Exception e)
				{
					NotifyFailure(new AerospikeException("Batch split retry failed", e));
					return;
				}
			}

			AsyncCommand command = CloneCommand();

			if (command != null)
			{
				// Command should only be added to AsyncTimeoutQueue once.
				// CheckTimeout() will verify both socketTimeout and totalTimeout.
				if (socketTimeout > 0)
				{
					command.socketWatch = ValueStopwatch.StartNew();
					AsyncTimeoutQueue.Instance.Add(command, socketTimeout);
				}
				else if (totalTimeout > 0)
				{
					int remain = (int)(totalTimeout - totalWatch.ElapsedMilliseconds);
					AsyncTimeoutQueue.Instance.Add(command, remain);
				}
				cluster.AddRetry();
				command.ExecuteCommand();
			}
			else
			{
				FailCommand(ae);
			}
		}

		/// <summary>
		/// Check for timeout from timeout queue thread.
		/// </summary>
		public bool CheckTimeout()
		{
			if (state != IN_PROGRESS)
			{
				return false;
			}

			// totalWatch is not initialized when totalTimeout is zero. Check if totalWatch is
			// initialized/active before checking totalTimeout.
			if (totalWatch.IsActive && totalWatch.ElapsedMilliseconds >= totalTimeout)
			{
				// Total timeout has occurred.
				if (Interlocked.CompareExchange(ref state, FAIL_TOTAL_TIMEOUT, IN_PROGRESS) == IN_PROGRESS)
				{
					// Close connection. This will result in a socket error in the async callback thread.
					if (node != null && conn != null)
					{
						node.CloseAsyncConnOnError(conn);
					}

					node?.AddTimeout(ns);

					// Notify user immediately in this timeout thread.
					// Command thread will cleanup eventArgs.
					NotifyFailure(new AerospikeException.Timeout(policy, true));
				}
				return false;  // Do not put back on timeout queue.
			}

			// socketWatch is not initialized for commands in the delay queue because the command
			// has not started yet. Check if socketWatch is initialized/active before checking
			// socketTimeout.
			if (socketWatch.IsActive && socketWatch.ElapsedMilliseconds >= socketTimeout)
			{
				if (eventReceived)
				{
					// Event(s) received within socket timeout period.
					eventReceived = false;
					socketWatch = ValueStopwatch.StartNew();
					return true;
				}

				// Socket timeout has occurred.
				if (Interlocked.CompareExchange(ref state, FAIL_SOCKET_TIMEOUT, IN_PROGRESS) == IN_PROGRESS)
				{
					// User will be notified in command thread and this timeout thread.
					// Close connection. This will result in a socket error in the async callback thread
					// and a possible retry.
					if (node != null && conn != null)
					{
						node.CloseAsyncConnOnError(conn);
					}

					node?.AddTimeout(ns);
				}
				return false;  // Do not put back on timeout queue.
			}
			return true; // Timeout not reached.
		}

		protected internal void Finish()
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref state, SUCCESS, IN_PROGRESS);

			if (status == IN_PROGRESS)
			{
				// Command finished successfully.
				// Put connection back into pool.
				AddBytesInOut();
				node.PutAsyncConnection(conn);
				ReleaseBuffer();
			}
			else if (status == FAIL_TOTAL_TIMEOUT)
			{
				// Timeout thread closed connection, but command still completed.
				// User has already been notified with timeout. Release buffer and return.
				ReleaseBuffer();
				return;
			}
			else if (status == FAIL_SOCKET_TIMEOUT)
			{
				// Timeout thread closed connection, but command still completed.
				// User has not been notified of the timeout. Release buffer and let
				// OnSuccess() be called.
				ReleaseBuffer();
			}
			else
			{
				// Should never get here.
				if (Log.WarnEnabled())
				{
					Log.Warn(cluster.context, "AsyncCommand finished with unexpected return status: " + status);
				}
			}

			LatencyType latencyType = cluster.MetricsEnabled ? GetLatencyType() : LatencyType.NONE;
			if (latencyType != LatencyType.NONE)
			{
				node.AddLatency(ns, latencyType, metricsWatch.Elapsed.TotalMilliseconds);
			}

			try
			{
				OnSuccess();
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(cluster.context, "OnSuccess() error: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void FailOnApplicationError(AerospikeException ae)
		{
			try
			{
				// Ensure that command succeeds or fails, but not both.
				int status = Interlocked.CompareExchange(ref state, FAIL_APPLICATION_ERROR, IN_PROGRESS);

				if (status == IN_PROGRESS)
				{
					if (node != null && conn != null)
					{
						if (ae.KeepConnection())
						{
							// Put connection back in pool.
							AddBytesInOut();
							node.PutAsyncConnection(conn);
						}
						else
						{
							// Close socket to flush out possible garbage.
							CloseConnection();
						}
					}
					FailCommand(ae);
				}
				else
				{
					AlreadyCompleted(status);
				}
			}
			catch (Exception e)
			{
				Log.Error(cluster.context, "FailOnApplicationError failed: " + Util.GetErrorMessage(e) +
					System.Environment.NewLine + "Original error: " + Util.GetErrorMessage(ae));
			}
		}

		internal void FailOnQueueError(AerospikeException ae)
		{
			int status = Interlocked.CompareExchange(ref state, FAIL_QUEUE_ERROR, IN_PROGRESS);

			if (status == IN_PROGRESS)
			{
				NotifyFailure(ae);
			}
		}

		private void AlreadyCompleted(int status)
		{
			// Only need to release resources from AsyncTimeoutQueue timeout.
			// Otherwise, resources have already been released.
			if (status == FAIL_TOTAL_TIMEOUT)
			{
				// Free up resources. Connection should have already been closed and user
				// notified in CheckTimeout().
				ReleaseBuffer();
			}
			else if (status == FAIL_SOCKET_TIMEOUT)
			{
				// Connection should have already been closed on AsyncTimeoutQueue timeout.
				AerospikeException timeoutException = new AerospikeException.Timeout(policy, true);

				if (iteration <= maxRetries)
				{
					Retry(timeoutException);
				}
				else
				{
					FailCommand(timeoutException);
				}
			}
			else
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(cluster.context, "AsyncCommand unexpected return status: " + status);
				}
			}
		}

		private void FailCommand(AerospikeException ae)
		{
			ReleaseBuffer();
			NotifyFailure(ae);
		}

		private void NotifyFailure(AerospikeException ae)
		{
			try
			{
				ae.Node = node;
				ae.Policy = policy;
				ae.Iteration = iteration;
				ae.SetInDoubt(IsWrite(), commandSentCounter);
				
				if (ae.InDoubt)
				{
					OnInDoubt();
				}
				
				OnFailure(ae);
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(cluster.context, "OnFailure() error: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void CloseConnection()
		{
			if (conn != null)
			{
				AddBytesInOut();
				node.CloseAsyncConnOnError(conn);
				conn = null;
			}
		}

		private void AddBytesInOut()
		{
			if (node.AreMetricsEnabled())
			{
				node.AddBytesIn(ns, bytesIn);
				node.AddBytesOut(ns, bytesOut);
			}
			bytesIn = 0;
			bytesOut = 0;
		}

		internal void ReleaseBuffer()
		{
			if (segment != null)
			{
				// Do not put large buffers back into pool.
				if (segment.index < 0)
				{
					// Put back original buffer instead.
					segment = segmentOrig;
				}
				cluster.ReleaseBuffer(segment);
				segment = null;
			}
		}

		// Do nothing by default. Write commands will override this method.
		protected internal virtual void OnInDoubt()
		{

		}

		protected internal virtual bool RetryBatch()
		{
			return false;
		}

		protected internal virtual bool IsWrite()
		{
			return false;
		}

		protected internal abstract Node GetNode(Cluster cluster);
		protected abstract LatencyType GetLatencyType();
		protected internal abstract void WriteBuffer();
		protected internal abstract AsyncCommand CloneCommand();
		protected internal abstract void ParseCommand();
		protected internal abstract bool PrepareRetry(bool timeout);
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
