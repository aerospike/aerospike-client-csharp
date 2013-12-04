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
using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous command handler.
	/// </summary>
	public abstract class AsyncCommand : Command
	{
		private static readonly EventHandler<SocketAsyncEventArgs> SocketListener = new EventHandler<SocketAsyncEventArgs>(SocketHandler);

		protected internal AsyncConnection conn;
		protected internal readonly AsyncCluster cluster;
		protected internal AsyncNode node;
		private Stopwatch watch;
		protected internal int dataLength;
		private int timeout;
		private int iteration;
		private int complete;
		protected internal bool inHeader = true;

		public AsyncCommand(AsyncCluster cluster)
		{
			this.cluster = cluster;
		}

		public void Execute()
		{
			Policy policy = GetPolicy();
			timeout = policy.timeout;

			if (timeout > 0)
			{
				watch = Stopwatch.StartNew();
				AsyncTimeoutQueue.Instance.Add(this, timeout);
			}

			dataBuffer = cluster.GetByteBuffer();
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
				conn = node.GetAsyncConnection();

				if (conn == null)
				{
					conn = new AsyncConnection(node.address, cluster, this, SocketListener);

					if (!conn.ConnectAsync())
					{
						ConnectEvent(conn.Args);
					}
				}
				else
				{
					SocketAsyncEventArgs args = conn.SetCommand(this);
					ConnectEvent(args);
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

			// A zero sleepBetweenRetries results in a thread yield (not infinite sleep).
			Util.Sleep(policy.sleepBetweenRetries);

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
						command.ReceiveEvent(args);
						break;
					case SocketAsyncOperation.Send:
						command.SendEvent(args);
						break;
					case SocketAsyncOperation.Connect:
						command.ConnectEvent(args);
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

		private void ConnectEvent(SocketAsyncEventArgs args)
		{
			if (complete != 0)
			{
				FailOnClientTimeout();
				return;
			}
			WriteBuffer();
			dataOffset = 0;
			args.SetBuffer(dataBuffer, dataOffset, dataLength);
			Send(args);
		}

		protected internal sealed override void SizeBuffer()
		{
			dataLength = dataOffset;

			if (dataLength > dataBuffer.Length)
			{
				dataBuffer = new byte[dataLength];
			}
		}

		private void Send(SocketAsyncEventArgs args)
		{
			if (! conn.SendAsync(args))
			{
				SendEvent(args);
			}
		}

		private void SendEvent(SocketAsyncEventArgs args)
		{
			dataOffset += args.BytesTransferred;

			if (dataOffset < dataLength)
			{
				args.SetBuffer(dataOffset, dataLength - dataOffset);
				Send(args);
			}
			else
			{
				ReceiveBegin(args);
			}
		}

		protected internal void ReceiveBegin(SocketAsyncEventArgs args)
		{
			dataOffset = 0;
			dataLength = 8;
			args.SetBuffer(dataOffset, dataLength);
			Receive(args);
		}

		private void Receive(SocketAsyncEventArgs args)
		{
			if (! conn.ReceiveAsync(args))
			{
				ReceiveEvent(args);
			}
		}

		private void ReceiveEvent(SocketAsyncEventArgs args)
		{
			//Log.Info("Receive Event: " + args.BytesTransferred + "," + dataOffset + "," + dataLength + "," + inHeader);

			if (args.BytesTransferred <= 0)
			{
				FailOnNetworkError(new AerospikeException.Connection("Connection closed"));
				return;
			}

			dataOffset += args.BytesTransferred;

			if (dataOffset < dataLength)
			{
				args.SetBuffer(dataOffset, dataLength - dataOffset);
				Receive(args);
				return;
			}
			dataOffset = 0;

			if (inHeader)
			{
				dataLength = (int)(ByteUtil.BytesToLong(dataBuffer, 0) & 0xFFFFFFFFFFFFL);

				if (dataLength <= 0)
				{
					Finish();
					return;
				}

				inHeader = false;

				if (dataLength > dataBuffer.Length)
				{
					dataBuffer = new byte[dataLength];
					args.SetBuffer(dataBuffer, dataOffset, dataLength);
				}
				else
				{
					args.SetBuffer(dataOffset, dataLength);
				}
				Receive(args);
			}
			else
			{
				ParseCommand(args);
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

			// A zero sleepBetweenRetries results in a thread yield (not infinite sleep).
			Util.Sleep(policy.sleepBetweenRetries);

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
			if (node != null)
			{
				node.DecreaseHealth();
			}

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
				node.RestoreHealth();
				cluster.PutByteBuffer(dataBuffer);
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
				Close();
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
			if (node != null)
			{
				node.DecreaseHealth();
			}
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
			cluster.PutByteBuffer(dataBuffer);
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
		protected internal abstract void ParseCommand(SocketAsyncEventArgs args);
		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}