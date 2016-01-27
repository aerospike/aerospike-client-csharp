/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	public abstract class AsyncCommand : Command
	{
		public static readonly EventHandler<SocketAsyncEventArgs> SocketListener = new EventHandler<SocketAsyncEventArgs>(SocketHandler);
		private const int SUCCESS = 1;
		private const int FAIL_TIMEOUT = 2;
		private const int FAIL_NETWORK_INIT = 3;
		private const int FAIL_NETWORK_ERROR = 4;
		private const int FAIL_APPLICATION_INIT = 5;
		private const int FAIL_APPLICATION_ERROR = 6;

		protected internal readonly AsyncCluster cluster;
		private AsyncConnection conn;
		private AsyncNode node;
		private SocketAsyncEventArgs eventArgs;
		private BufferSegment segmentOrig;
		private BufferSegment segment;
		private Stopwatch watch;
		protected internal int dataLength;
		private int timeout;
		private int iteration;
		private int complete;
		private int failedNodes;
		private int failedConns;
		private bool inAuthenticate;
		protected internal bool inHeader = true;

		public AsyncCommand(AsyncCluster cluster)
		{
			this.cluster = cluster;
		}

		public void Execute()
		{
			eventArgs = cluster.GetEventArgs();
			segment = segmentOrig = eventArgs.UserToken as BufferSegment;
			eventArgs.UserToken = this;

			if (cluster.HasBufferChanged(segment))
			{
				// Reset buffer in SizeBuffer().
				segment.buffer = null;
				segment.offset = 0;
				segment.size = 0;
			}

			Policy policy = GetPolicy();
			timeout = policy.timeout;

			if (timeout > 0)
			{
				watch = Stopwatch.StartNew();
				AsyncTimeoutQueue.Instance.Add(this, timeout);
			}

			ExecuteCommand();
		}

		private void ExecuteCommand()
		{
			if (complete != 0)
			{
				AlreadyCompleted(complete);
				return;
			}

			try
			{
				node = GetNode();
				eventArgs.RemoteEndPoint = node.address;

				conn = node.GetAsyncConnection();

				if (conn == null)
				{
					conn = new AsyncConnection(node.address, cluster);
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
			}
			catch (AerospikeException.InvalidNode)
			{
				failedNodes++;
				if (!RetryOnInit())
				{
					throw;
				}
			}
			catch (AerospikeException.Connection)
			{
				failedConns++;
				if (!RetryOnInit())
				{
					throw;
				}
			}
			catch (SocketException se)
			{
				if (!RetryOnInit())
				{
					throw GetAerospikeException(se.SocketErrorCode);
				}
			}
			catch (Exception e)
			{
				if (!FailOnApplicationInit())
				{
					throw new AerospikeException(e);
				}
			}
		}

		private bool RetryOnInit()
		{
			if (complete != 0)
			{
				AlreadyCompleted(complete);
				return true;
			}

			Policy policy = GetPolicy();
			
			if (++iteration > policy.maxRetries)
			{
				return FailOnNetworkInit();
			}

			// Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.
			//if (watch != null && (watch.ElapsedMilliseconds + policy.sleepBetweenRetries) > timeout)
			if (watch != null && watch.ElapsedMilliseconds > timeout)
			{
				// Might as well stop here because the transaction will
				// timeout after sleep completed.
				return FailOnNetworkInit();
			}

			// Prepare for retry.
			ResetConnection();

			// Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.
			//if (policy.sleepBetweenRetries > 0)
			//{
			//	Util.Sleep(policy.sleepBetweenRetries);
			//}

			// Retry command recursively.
			ExecuteCommand();
			return true;
		}

		static void SocketHandler(object sender, SocketAsyncEventArgs args)
		{
			AsyncCommand command = args.UserToken as AsyncCommand;

			if (args.SocketError != SocketError.Success)
			{
				command.RetryAfterInit(command.GetAerospikeException(args.SocketError));
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
				command.RetryAfterInit(ac);
			}
			catch (AerospikeException e)
			{
				// Fail without retry on non-network errors.
				command.FailOnApplicationError(e);
			}
			catch (SocketException se)
			{
				command.RetryAfterInit(command.GetAerospikeException(se.SocketErrorCode));
			}
			catch (Exception e)
			{
				// Fail without retry on unknown errors.
				command.FailOnApplicationError(new AerospikeException(e));
			}
		}

		private void ConnectionCreated()
		{
			if (complete != 0)
			{
				AlreadyCompleted(complete);
				return;
			}

			if (cluster.user != null)
			{
				inAuthenticate = true;
				// Authentication messages are small.  Set a reasonable upper bound.
				dataOffset = 200;
				SizeBuffer();

				AdminCommand command = new AdminCommand(dataBuffer, dataOffset);
				dataLength = command.SetAuthenticate(cluster.user, cluster.password);
				eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength - dataOffset);
				Send();
				return;
			}
			ConnectionReady();
		}

		private void ConnectionReady()
		{
			if (complete != 0)
			{
				AlreadyCompleted(complete);
				return;
			}
			WriteBuffer();
			eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength - dataOffset);
			Send();
		}

		protected internal sealed override void SizeBuffer()
		{
			// dataOffset is currently the estimate, which may be greater than the actual size.
			dataLength = dataOffset;

			if (dataLength > segment.size)
			{
				ResizeBuffer(dataLength);
			}
			dataBuffer = segment.buffer;
			dataOffset = segment.offset;
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

		private void Send()
		{
			if (! conn.SendAsync(eventArgs))
			{
				SendEvent();
			}
		}

		private void SendEvent()
		{
			dataOffset += eventArgs.BytesTransferred;

			if (dataOffset < dataLength)
			{
				eventArgs.SetBuffer(dataOffset, dataLength - dataOffset);
				Send();
			}
			else
			{
				ReceiveBegin();
			}
		}

		protected internal void ReceiveBegin()
		{
			dataOffset = segment.offset;
			dataLength = dataOffset + 8;
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

			if (eventArgs.BytesTransferred <= 0)
			{
				FailOnNetworkError(new AerospikeException.Connection("Connection closed"));
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
				int length = (int)(ByteUtil.BytesToLong(dataBuffer, dataOffset) & 0xFFFFFFFFFFFFL);

				if (length <= 0)
				{
					ReceiveBegin();
					return;
				}

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

					if (resultCode != 0)
					{
						throw new AerospikeException(resultCode);
					}
					ConnectionReady();
					return;
				}
				ParseCommand();
			}
		}

		private void RetryAfterInit(AerospikeException ae)
		{
			if (complete != 0)
			{
				AlreadyCompleted(complete);
				return;
			}

			Policy policy = GetPolicy();

			if (++iteration > policy.maxRetries)
			{
				FailOnNetworkError(ae);
				return;
			}

			// Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.
			//if (watch != null && (watch.ElapsedMilliseconds + policy.sleepBetweenRetries) > timeout)
			if (watch != null && watch.ElapsedMilliseconds > timeout)
			{
				// Might as well stop here because the transaction will
				// timeout after sleep completed.
				FailOnNetworkError(ae);
				return;
			}

			// Prepare for retry.
			ResetConnection();

			// Disable sleep between retries.  Sleeping in asynchronous mode makes no sense.
			//if (policy.sleepBetweenRetries > 0)
			//{
			//	Util.Sleep(policy.sleepBetweenRetries);
			//}

			try
			{
				// Retry command recursively.
				ExecuteCommand();
			}
			catch (Exception)
			{
				// Command has already been cleaned up.
				// Notify user of original exception.
				OnFailure(ae);
			}
		}

		private void ResetConnection()
		{
			if (watch != null)
			{
				// A lock on reset is required when a client timeout is specified.
				lock (this)
				{
					if (conn != null)
					{
						conn.Close();
						conn = null;
					}
				}
			}
			else
			{
				if (conn != null)
				{
					conn.Close();
					conn = null;
				}
			}
		}

		/// <summary>
		/// Check for timeout from timeout queue thread.
		/// </summary>
		protected internal bool CheckTimeout()
		{
			if (complete != 0)
			{
				return false;
			}

			if (watch.ElapsedMilliseconds > timeout)
			{
				// Command has timed out in timeout queue thread.
				// Ensure that command succeeds or fails, but not both.
				if (Interlocked.CompareExchange(ref complete, FAIL_TIMEOUT, 0) == 0)
				{
					// Timeout thread may contend with retry thread.
					// Lock before closing.
					lock (this)
					{
						if (conn != null)
						{
							conn.Close();
						}
					}
				}
				return false;  // Do not put back on timeout queue.
			}
			return true;  // Put back on timeout queue.
		}

		protected internal void Finish()
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref complete, SUCCESS, 0);

			if (status == 0)
			{
				conn.UpdateLastUsed();
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

				try
				{
					OnSuccess();
				}
				catch (AerospikeException ae)
				{
					// The user's OnSuccess() may have already been called which in turn generates this
					// exception.  It's important to call OnFailure() anyhow because the user's code 
					// may be waiting for completion notification which wasn't yet called in
					// OnSuccess().  This is the only case where both OnSuccess() and OnFailure()
					// gets called for the same command.
					OnFailure(ae);
				}
				catch (Exception e)
				{
					OnFailure(new AerospikeException(e));
				}
			}
			else
			{
				AlreadyCompleted(status);
			}
		}

		private bool FailOnNetworkInit()
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref complete, FAIL_NETWORK_INIT, 0);

			if (status == 0)
			{
				CloseOnError();
				return false;
			}
			else
			{
				AlreadyCompleted(status);
				return true;
			}
		}

		private bool FailOnApplicationInit()
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref complete, FAIL_APPLICATION_INIT, 0);

			if (status == 0)
			{
				CloseOnError();
				return false;
			}
			else
			{
				AlreadyCompleted(status);
				return true;
			}
		}

		private void FailOnNetworkError(AerospikeException ae)
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref complete, FAIL_NETWORK_ERROR, 0);

			if (status == 0)
			{
				CloseOnError();
				OnFailure(ae);
			}
			else
			{
				AlreadyCompleted(status);
			}
		}

		private void FailOnApplicationError(AerospikeException ae)
		{
			// Ensure that command succeeds or fails, but not both.
			int status = Interlocked.CompareExchange(ref complete, FAIL_APPLICATION_ERROR, 0);

			if (status == 0)
			{
				if (ae.KeepConnection())
				{
					// Put connection back in pool.
					conn.UpdateLastUsed();
					node.PutAsyncConnection(conn);
					PutBackArgsOnError();
				}
				else
				{
					// Close socket to flush out possible garbage.
					CloseOnError();
				}
				OnFailure(ae);
			}
			else
			{
				AlreadyCompleted(status);
			}
		}

		private void AlreadyCompleted(int status)
		{
			// Only need to release resources from AsyncTimeoutQueue timeout.
			// Otherwise, resources have already been released.
			if (status == FAIL_TIMEOUT)
			{
				// Free up resources and notify user on timeout.
				// Connection should have already been closed on AsyncTimeoutQueue timeout.
				PutBackArgsOnError();
				OnFailure(new AerospikeException.Timeout(node, timeout, iteration, failedNodes, failedConns));
			}
			else
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("AsyncCommand unexpected return status: " + status);
				}
			}
		}

		private void CloseOnError()
		{
			// Connection may be null because never obtained connection or connection
			// was reset in preparation for retry.
			if (conn != null)
			{
				conn.Close();
			}
			PutBackArgsOnError();
		}

		private void PutBackArgsOnError()
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
				// There may be rare error cases where dataBuffer and eventArgs.Buffer
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
				return new AerospikeException.Timeout(node, timeout, iteration, failedNodes, failedConns);
			}
			return new AerospikeException.Connection("Socket error: " + se);
		}

		protected internal abstract AsyncNode GetNode();
		protected internal abstract void ParseCommand();
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
