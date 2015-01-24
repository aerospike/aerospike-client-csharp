/* 
 * Copyright 2012-2014 Aerospike, Inc.
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

		protected internal readonly AsyncCluster cluster;
		private AsyncConnection conn;
		private AsyncNode node;
		private SocketAsyncEventArgs eventArgs;
		private Stopwatch watch;
		private byte[] oldDataBuffer;
		protected internal int dataLength;
		private int timeout;
		private int iteration;
		private int complete;
		private bool inAuthenticate;
		protected internal bool inHeader = true;

		public AsyncCommand(AsyncCluster cluster)
		{
			this.cluster = cluster;
		}

		public void Execute()
		{
			eventArgs = cluster.GetEventArgs();
			eventArgs.UserToken = this;
			dataBuffer = eventArgs.Buffer;

			if (dataBuffer != null && cluster.HasBufferChanged(dataBuffer))
			{
				// Reset dataBuffer in SizeBuffer().
				dataBuffer = null;
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
				FailOnClientTimeout();
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
					eventArgs.SetBuffer(0, 0);

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
				if (!RetryOnInit())
				{
					throw;
				}
			}
			catch (AerospikeException.Connection)
			{
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
				FailOnClientTimeout();
				return true;
			}

			Policy policy = GetPolicy();
			
			if (++iteration > policy.maxRetries)
			{
				return FailOnNetworkInit();
			}

			if (watch != null && (watch.ElapsedMilliseconds + policy.sleepBetweenRetries) > timeout)
			{
				// Might as well stop here because the transaction will
				// timeout after sleep completed.
				return FailOnNetworkInit();
			}

			// Prepare for retry.
			ResetConnection();

			if (policy.sleepBetweenRetries > 0)
			{
				Util.Sleep(policy.sleepBetweenRetries);
			}

			// Retry command recursively.
			ExecuteCommand();
			return true;
		}

		static void SocketHandler(object sender, SocketAsyncEventArgs args)
		{
			AsyncCommand command = args.UserToken as AsyncCommand;

			if (args.SocketError != SocketError.Success)
			{
				command.RetryAfterInit(GetAerospikeException(args.SocketError));
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
				command.RetryAfterInit(GetAerospikeException(se.SocketErrorCode));
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
				FailOnClientTimeout();
				return;
			}

			if (cluster.user != null)
			{
				inAuthenticate = true;
				// Authentication messages are small.  Set a reasonable upper bound.
				dataOffset = 200;
				SizeBuffer();

				AdminCommand command = new AdminCommand(dataBuffer);
				dataLength = command.SetAuthenticate(cluster.user, cluster.password);		 
				dataOffset = 0;
				eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength);
				Send();
				return;
			}
			ConnectionReady();
		}

		private void ConnectionReady()
		{
			if (complete != 0)
			{
				FailOnClientTimeout();
				return;
			}
			WriteBuffer();
			dataOffset = 0;
			eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength);
			Send();
		}

		protected internal sealed override void SizeBuffer()
		{
			dataLength = dataOffset;

			if (dataBuffer == null || dataLength > dataBuffer.Length)
			{
				ResizeBuffer();
			}
		}

		private void ResizeBuffer()
		{
			if (dataLength <= BufferPool.BUFFER_CUTOFF)
			{
				// Checkout buffer from cache.
				dataBuffer = cluster.GetNextBuffer(dataLength);
			}
			else
			{
				// Large buffers should not be cached.
				// Allocate, but do not put back into pool.
				if (dataBuffer != null && dataBuffer.Length <= BufferPool.BUFFER_CUTOFF)
				{
					oldDataBuffer = dataBuffer;
				}
				dataBuffer = new byte[dataLength];
			}
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
			dataOffset = 0;
			dataLength = 8;
			eventArgs.SetBuffer(dataOffset, dataLength);
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
			dataOffset = 0;

			if (inHeader)
			{
				dataLength = (int)(ByteUtil.BytesToLong(dataBuffer, 0) & 0xFFFFFFFFFFFFL);

				if (dataLength <= 0)
				{
					throw new AerospikeException.Parse("Received zero sized data packet from server.");
				}

				inHeader = false;

				if (dataLength > dataBuffer.Length)
				{
					ResizeBuffer();
				}
				eventArgs.SetBuffer(dataBuffer, dataOffset, dataLength);
				Receive();
			}
			else
			{
				if (inAuthenticate)
				{
					inAuthenticate = false;
					inHeader = true;

					int resultCode = dataBuffer[1];

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
				FailOnClientTimeout();
				return;
			}

			Policy policy = GetPolicy();

			if (++iteration > policy.maxRetries)
			{
				FailOnNetworkError(ae);
				return;
			}

			if (watch != null && (watch.ElapsedMilliseconds + policy.sleepBetweenRetries) > timeout)
			{
				// Might as well stop here because the transaction will
				// timeout after sleep completed.
				FailOnNetworkError(ae);
				return;
			}

			// Prepare for retry.
			ResetConnection();

			if (policy.sleepBetweenRetries > 0)
			{
				Util.Sleep(policy.sleepBetweenRetries);
			}

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
				if (Interlocked.Exchange(ref complete, 1) == 0)
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
			if (Interlocked.Exchange(ref complete, 1) == 0)
			{
				conn.UpdateLastUsed();
				node.PutAsyncConnection(conn);

				// Do not put large buffers back into pool.
				if (dataBuffer.Length > BufferPool.BUFFER_CUTOFF)
				{
					// Put back original buffer instead.
					eventArgs.SetBuffer(oldDataBuffer, 0, 0);
				}

				cluster.PutEventArgs(eventArgs);
				OnSuccess();
			}
			else
			{
				FailOnClientTimeout();
			}
		}

		private bool FailOnNetworkInit()
		{
			// Ensure that command succeeds or fails, but not both.
			if (Interlocked.Exchange(ref complete, 1) == 0)
			{
				CloseOnNetworkError();
				return false;
			}
			else
			{
				FailOnClientTimeout();
				return true;
			}
		}

		private bool FailOnApplicationInit()
		{
			// Ensure that command succeeds or fails, but not both.
			if (Interlocked.Exchange(ref complete, 1) == 0)
			{
				Close();
				return false;
			}
			else
			{
				FailOnClientTimeout();
				return true;
			}
		}

		private void FailOnNetworkError(AerospikeException ae)
		{
			// Ensure that command succeeds or fails, but not both.
			if (Interlocked.Exchange(ref complete, 1) == 0)
			{
				CloseOnNetworkError();
				OnFailure(ae);
			}
			else
			{
				FailOnClientTimeout();
			}
		}

		private void FailOnApplicationError(AerospikeException ae)
		{
			// Ensure that command succeeds or fails, but not both.
			if (Interlocked.Exchange(ref complete, 1) == 0)
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
					Close();
				}
				OnFailure(ae);
			}
			else
			{
				FailOnClientTimeout();
			}
		}

		private void FailOnClientTimeout()
		{
			// Free up resources and notify.
			CloseOnNetworkError();
			OnFailure(new AerospikeException.Timeout());
		}

		private void CloseOnNetworkError()
		{
			Close();
		}

		private void Close()
		{
			// Connection was probably already closed by timeout thread.
			// Check connected status before closing again.
			if (conn != null && conn.IsConnected())
			{
				conn.Close();
			}

			PutBackArgsOnError();
		}

		private void PutBackArgsOnError()
		{
			// Do not put large buffers back into pool.
			if (dataBuffer != null && dataBuffer.Length > BufferPool.BUFFER_CUTOFF)
			{
				// Put back original buffer instead.
				eventArgs.SetBuffer(oldDataBuffer, 0, 0);
			}
			else
			{
				// There may be rare error cases where dataBuffer and eventArgs.Buffer
				// are different.  Make sure they are in sync.
				eventArgs.SetBuffer(dataBuffer, 0, 0);
			}

			cluster.PutEventArgs(eventArgs);
		}

		private static AerospikeException GetAerospikeException(SocketError se)
		{
			if (se == SocketError.TimedOut)
			{
				return new AerospikeException.Timeout();
			}
			return new AerospikeException.Connection("Socket error: " + se);
		}

		protected internal abstract AsyncNode GetNode();
		protected internal abstract void ParseCommand();
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
