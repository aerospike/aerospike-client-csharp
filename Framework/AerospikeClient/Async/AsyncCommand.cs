/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous command handler.
	/// </summary>
	public abstract class AsyncCommand : Command, ITimeout
	{
		public static EventHandler<SocketAsyncEventArgs> SocketListener { get { return EventHandlers.SocketHandler; } }
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
		private SocketAsyncEventArgs eventArgs;
		private BufferSegment segmentOrig;
		private BufferSegment segment;
		private Stopwatch socketWatch;
		private Stopwatch totalWatch;
		protected internal int dataLength;
		private int iteration;
		protected internal int commandSentCounter;
		private volatile int state;
		private volatile bool eventReceived;
		private bool compressed;
		private bool inAuthenticate;
		protected internal bool inHeader = true;

		/// <summary>
		/// Default Constructor.
		/// </summary>
		public AsyncCommand(AsyncCluster cluster, Policy policy)
			: base(policy.socketTimeout, policy.totalTimeout, policy.maxRetries)
		{
			this.cluster = cluster;
			this.policy = policy;
		}

		/// <summary>
		/// Scan/Query Constructor.
		/// </summary>
		public AsyncCommand(AsyncCluster cluster, Policy policy, int socketTimeout, int totalTimeout)
			: base(socketTimeout, totalTimeout, 0)
		{
			this.cluster = cluster;
			this.policy = policy;
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
			this.eventArgs = other.eventArgs;
			this.eventArgs.UserToken = this;
			this.segmentOrig = other.segmentOrig;
			this.segment = other.segment;
			this.totalWatch = other.totalWatch;
			this.iteration = other.iteration;
			this.commandSentCounter = other.commandSentCounter;
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
				totalWatch = Stopwatch.StartNew();
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

		// Executes the command from the thread pool, using the specified SocketAsyncEventArgs object.
		internal void ExecuteAsync(SocketAsyncEventArgs e)
		{
			eventArgs = e;
			ThreadPool.UnsafeQueueUserWorkItem(EventHandlers.AsyncExecuteHandler, this);
		}

		// Executes the command on the current thread, using the specified SocketAsyncEventArgs object.
		internal void ExecuteInline(SocketAsyncEventArgs e)
		{
			// Use global consective error count to determine if it's safe to run command immediately.
			// ErrorCount does not need to be atomic because it's just a general indicator that stack
			// recursion may get out of control.  Absolute accuracy is not required in this case.
			if (ErrorCount < 5)
			{
				// Execute command now.
				eventArgs = e;
				ExecuteCore();
			}
			else
			{
				// Prevent recursive error stack overflow by placing command in a queue.
				ExecuteAsync(e);
			}
		}

		// Actually executes the command, once the event args and the thread issues have all been sorted out.
		private void ExecuteCore()
		{
			segment = segmentOrig = eventArgs.UserToken as BufferSegment;
			eventArgs.UserToken = this;

			if (cluster.HasBufferChanged(segment))
			{
				// Reset buffer in SizeBuffer().
				segment.buffer = null;
				segment.offset = 0;
				segment.size = 0;
			}

			if (totalTimeout > 0)
			{
				// Timeout already added in Execute(). Verify state.
				if (state != IN_PROGRESS)
				{
					// Timeout occurred. Close command.
					PutBackArgsOnError();
					return;
				}

				if (socketTimeout > 0)
				{
					// socketTimeout is an idle timeout. socketWatch is restarted on every attempt.
					socketWatch = Stopwatch.StartNew();
				}
			}
			else if (socketTimeout > 0)
			{
				socketWatch = Stopwatch.StartNew();
				AsyncTimeoutQueue.Instance.Add(this, socketTimeout);
			}
			ExecuteCommand();
		}

		private void ExecuteCommand()
		{
			iteration++;

			try
			{
				node = (AsyncNode)GetNode(cluster);
				node.ValidateErrorCount();
				eventArgs.RemoteEndPoint = node.address;

				conn = node.GetAsyncConnection();

				if (conn == null)
				{
					node.IncrAsyncConnTotal();
					conn = new AsyncConnection(node.address, node);
					eventArgs.SetBuffer(segment.buffer, segment.offset, 0);

					if (!conn.ConnectAsync(eventArgs))
					{
						ConnectionCreated();
					}
				}
				else
				{
					ConnectionReady();
				}
				ErrorCount = 0;
			}
			catch (AerospikeException.Connection aec)
			{
				ErrorCount++;
				ConnectionFailed(aec);
			}
			catch (AerospikeException.Backoff aeb)
			{
				ErrorCount++;
				Backoff(aeb);
			}
			catch (AerospikeException ae)
			{
				ErrorCount++;
				FailOnApplicationError(ae);
			}
			catch (SocketException se)
			{
				ErrorCount++;
				ConnectionFailed(GetAerospikeException(se.SocketErrorCode));
			}
			catch (Exception e)
			{
				ErrorCount++;
				FailOnApplicationError(new AerospikeException(e));
			}
		}

		// Wrap the stateless event handlers in an instance, in order to avoid static delegate performance penalty.
		private sealed class EventHandlers
		{
			private static readonly EventHandlers Instance = new EventHandlers();

			public static readonly WaitCallback AsyncExecuteHandler = Instance.HandleExecution;

			public static readonly EventHandler<SocketAsyncEventArgs> SocketHandler = Instance.HandleSocketEvent;

			private EventHandlers() { }

			private void HandleExecution(object state)
			{
				((AsyncCommand)state).ExecuteCore();
			}

			private void HandleSocketEvent(object sender, SocketAsyncEventArgs args)
			{
				AsyncCommand command = args.UserToken as AsyncCommand;

				if (args.SocketError != SocketError.Success)
				{
					command.ConnectionFailed(command.GetAerospikeException(args.SocketError));
					return;
				}

				try
				{
					switch (args.LastOperation)
					{
						case SocketAsyncOperation.Receive:
							command.ReceiveEvent();
							break;
						case SocketAsyncOperation.Send:
							command.SendEvent();
							break;
						case SocketAsyncOperation.Connect:
							command.ConnectionCreated();
							break;
						default:
							command.FailOnApplicationError(new AerospikeException("Invalid socket operation: " + args.LastOperation));
							break;
					}
				}
				catch (AerospikeException.Connection ac)
				{
					command.ConnectionFailed(ac);
				}
				catch (AerospikeException ae)
				{
					if (ae.Result == ResultCode.TIMEOUT)
					{
						command.RetryServerError(new AerospikeException.Timeout(command.policy, false));
					}
					else if (ae.Result == ResultCode.DEVICE_OVERLOAD)
					{
						command.RetryServerError(ae);
					}
					else
					{
						command.FailOnApplicationError(ae);
					}
				}
				catch (SocketException se)
				{
					command.ConnectionFailed(command.GetAerospikeException(se.SocketErrorCode));
				}
				catch (ObjectDisposedException ode)
				{
					// This exception occurs because socket is being used after timeout thread closes socket.
					// Retry when this happens.
					command.ConnectionFailed(new AerospikeException(ode));
				}
				catch (Exception e)
				{
					// Fail without retry on unknown errors.
					command.FailOnApplicationError(new AerospikeException(e));
				}
			}
		}

		private void ConnectionCreated()
		{
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
					eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength - dataOffset);
					Send();
					return;
				}
			}
			ConnectionReady();
		}

		private void ConnectionReady()
		{
			WriteBuffer();
			eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength - dataOffset);
			Send();
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
			if (size <= BufferPool.BUFFER_CUTOFF)
			{
				// Checkout buffer from cache.
				cluster.GetNextBuffer(size, segment);
			}
			else
			{
				// Large buffers should not be cached.
				// Allocate, but do not put back into pool.
				segment = new BufferSegment();
				segment.buffer = new byte[size];
				segment.offset = 0;
				segment.size = size;
			}
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

		private void Send()
		{
			if (! conn.SendAsync(eventArgs))
			{
				SendEvent();
			}
		}

		private void SendEvent()
		{
			int sent = eventArgs.BytesTransferred;

			if (sent <= 0)
			{
				// When a node has shutdown on linux, async command send events return zero
				// with SocketError.Success. If zero bytes sent on send, cancel command.
				ConnectionFailed(new AerospikeException.Connection("Connection closed"));
				return;
			}

			dataOffset += sent;

			if (dataOffset < dataLength)
			{
				eventArgs.SetBuffer(dataOffset, dataLength - dataOffset);
				Send();
			}
			else
			{
				commandSentCounter++;

				if (socketWatch != null)
				{
					eventReceived = false;
				}
				ReceiveBegin();
			}
		}

		protected internal void ReceiveBegin()
		{
			dataOffset = segment.offset;
			dataLength = dataOffset + 8;
			dataBuffer = eventArgs.Buffer;
			eventArgs.SetBuffer(dataOffset, 8);
			Receive();
		}

		private void Receive()
		{
			if (! conn.ReceiveAsync(eventArgs))
			{
				ReceiveEvent();
			}
		}

		private void ReceiveEvent()
		{
			//Log.Info("Receive Event: " + eventArgs.BytesTransferred + "," + dataOffset + "," + dataLength + "," + inHeader);
			if (socketWatch != null)
			{
				eventReceived = true;
			}

			if (eventArgs.BytesTransferred <= 0)
			{
				ConnectionFailed(new AerospikeException.Connection("Connection closed"));
				return;
			}

			dataOffset += eventArgs.BytesTransferred;

			if (dataOffset < dataLength)
			{
				eventArgs.SetBuffer(dataOffset, dataLength - dataOffset);
				Receive();
				return;
			}
			dataOffset = segment.offset;

			if (inHeader)
			{
				long proto = ByteUtil.BytesToLong(dataBuffer, dataOffset);
				int length = (int)(proto & 0xFFFFFFFFFFFFL);

				if (length <= 0)
				{
					ReceiveBegin();
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
				eventArgs.SetBuffer(dataBuffer, dataOffset, length);
				dataLength = dataOffset + length;
				Receive();
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
						// transactions do not fail.
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

				conn.UpdateLastUsed();

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

		private void ConnectionFailed(AerospikeException ae)
		{
			if (iteration <= maxRetries && (totalWatch == null || totalWatch.ElapsedMilliseconds < totalTimeout))
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
			node.IncrErrorCount();

			if (iteration <= maxRetries && (totalWatch == null || totalWatch.ElapsedMilliseconds < totalTimeout))
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
			if (iteration <= maxRetries && (totalWatch == null || totalWatch.ElapsedMilliseconds < totalTimeout))
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
					command.socketWatch = Stopwatch.StartNew();
					AsyncTimeoutQueue.Instance.Add(command, socketTimeout);
				}
				else if (totalTimeout > 0)
				{
					int remain = (int)(totalTimeout - totalWatch.ElapsedMilliseconds);
					AsyncTimeoutQueue.Instance.Add(command, remain);
				}
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

			Stopwatch watch = totalWatch;

			if (watch != null && watch.ElapsedMilliseconds >= totalTimeout)
			{
				// Total timeout has occurred.
				if (Interlocked.CompareExchange(ref state, FAIL_TOTAL_TIMEOUT, IN_PROGRESS) == IN_PROGRESS)
				{
					// Close connection. This will result in a socket error in the async callback thread.
					if (node != null && conn != null)
					{
						node.CloseAsyncConnOnError(conn);
					}

					// Notify user immediately in this timeout thread.
					// Transaction thread will cleanup eventArgs.
					NotifyFailure(new AerospikeException.Timeout(policy, true));
				}
				return false;  // Do not put back on timeout queue.
			}

			watch = socketWatch;

			if (watch != null && watch.ElapsedMilliseconds >= socketTimeout)
			{
				if (eventReceived)
				{
					// Event(s) received within socket timeout period.
					eventReceived = false;
					socketWatch = Stopwatch.StartNew();
					return true;
				}

				// Socket timeout has occurred.
				if (Interlocked.CompareExchange(ref state, FAIL_SOCKET_TIMEOUT, IN_PROGRESS) == IN_PROGRESS)
				{
					// User will be notified in transaction thread and this timeout thread. 
					// Close connection. This will result in a socket error in the async callback thread
					// and a possible retry.
					if (node != null && conn != null)
					{
						node.CloseAsyncConnOnError(conn);
					}
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
				node.PutAsyncConnection(conn);

				// Do not put large buffers back into pool.
				if (segment.size > BufferPool.BUFFER_CUTOFF)
				{
					// Put back original buffer instead.
					segment = segmentOrig;
					eventArgs.SetBuffer(segment.buffer, segment.offset, 0);
				}

				eventArgs.UserToken = segment;
				cluster.PutEventArgs(eventArgs);
			}
			else if (status == FAIL_TOTAL_TIMEOUT || status == FAIL_SOCKET_TIMEOUT)
			{
				// Timeout thread closed connection, but transaction still completed. 
				// Do not put connection back into pool.
				PutBackArgsOnError();
			}
			else
			{
				// Should never get here.
				if (Log.WarnEnabled())
				{
					Log.Warn("AsyncCommand finished with unexpected return status: " + status);
				}
			}

			try
			{
				OnSuccess();
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("OnSuccess() error: " + Util.GetErrorMessage(e));
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
				Log.Error("FailOnApplicationError failed: " + Util.GetErrorMessage(e) +
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
				PutBackArgsOnError();
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
					Log.Warn("AsyncCommand unexpected return status: " + status);
				}
			}
		}

		private void FailCommand(AerospikeException ae)
		{
			PutBackArgsOnError();
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
				OnFailure(ae);
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("OnFailure() error: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void CloseConnection()
		{
			if (conn != null)
			{
				node.CloseAsyncConnOnError(conn);
				conn = null;
			}
		}

		internal void PutBackArgsOnError()
		{
			// Do not put large buffers back into pool.
			if (segment.size > BufferPool.BUFFER_CUTOFF)
			{
				// Put back original buffer instead.
				segment = segmentOrig;
				eventArgs.SetBuffer(segment.buffer, segment.offset, 0);
			}
			else
			{
				// There may be rare error cases where segment.buffer and eventArgs.Buffer
				// are different.  Make sure they are in sync.
				if (eventArgs.Buffer != segment.buffer)
				{
					eventArgs.SetBuffer(segment.buffer, segment.offset, 0);
				}
			}
			eventArgs.UserToken = segment;
			cluster.PutEventArgs(eventArgs);
		}

		private AerospikeException GetAerospikeException(SocketError se)
		{
			if (se == SocketError.TimedOut)
			{
				return new AerospikeException.Timeout(policy, true);
			}
			return new AerospikeException.Connection("Socket error: " + se);
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
		protected internal abstract void WriteBuffer();
		protected internal abstract AsyncCommand CloneCommand();
		protected internal abstract void ParseCommand();
		protected internal abstract bool PrepareRetry(bool timeout);
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
