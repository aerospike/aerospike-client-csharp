/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class AsyncConnectorExecutor : ConnectorListener
	{
		private readonly AsyncCluster cluster;
		private readonly AsyncNode node;
		private readonly int maxConnections;
		private readonly int maxConcurrent;
		private int countConnections;
		private readonly bool wait;
		private bool completed;

		public AsyncConnectorExecutor
		(
			AsyncCluster cluster,
			AsyncNode node,
			int maxConnections,
			int maxConcurrent,
			bool wait
		)
		{
			this.cluster = cluster;
			this.node = node;
			this.maxConnections = maxConnections;
			this.maxConcurrent = (maxConnections >= maxConcurrent) ? maxConcurrent : maxConnections;
			this.wait = wait;

			for (int i = 0; i < this.maxConcurrent; i++)
			{
				try
				{
					SocketAsyncEventArgs eventArgs = new SocketAsyncEventArgs();
					eventArgs.RemoteEndPoint = node.address;
					eventArgs.Completed += AsyncConnector.SocketListener;

					new AsyncConnector(cluster, node, this, eventArgs);
				}
				catch (Exception e)
				{
					OnFailure("Node " + node + " failed to create connection: " + e.Message);
					return;
				}
			}

			if (wait)
			{
				WaitTillComplete();
			}
		}

		public void OnSuccess(SocketAsyncEventArgs eventArgs)
		{
			int count = Interlocked.Increment(ref countConnections);

			if (count < maxConnections)
			{
				int next = count + maxConcurrent - 1;

				// Determine if a new command needs to be started.
				if (next < maxConnections && !completed)
				{
					// Create next connection.
					try
					{
						new AsyncConnector(cluster, node, this, eventArgs);
					}
					catch (Exception e)
					{
						OnFailure("Node " + node + " failed to create connection: " + e.Message);
					}
				}
			}
			else
			{
				// Ensure executor succeeds or fails exactly once.
				Complete();
			}
		}

		public void OnFailure(string error)
		{
			// Connection failed.  Highly unlikely other connections will succeed.
			// Abort the process.
			if (Log.DebugEnabled())
			{
				Log.Debug(error);
			}
			Complete();
		}

		private void Complete()
		{
			if (wait)
			{
				NotifyCompleted();
			}
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}

	public sealed class AsyncConnector : ITimeout
	{
		internal static EventHandler<SocketAsyncEventArgs> SocketListener {get {return ConnectorHandlers.SocketHandler;}}
		
		private readonly AsyncCluster cluster;
		private readonly AsyncNode node;
		private readonly ConnectorListener listener;
		private readonly SocketAsyncEventArgs eventArgs;
		private readonly byte[] sessionToken;
		private readonly byte[] dataBuffer;
		private readonly Stopwatch watch;
		private AsyncConnection conn;
		private int state;
		private int dataOffset;
		private int dataLength;
		private bool inHeader = true;

		public AsyncConnector(
			AsyncCluster cluster,
			AsyncNode node,
			ConnectorListener listener,
			SocketAsyncEventArgs args
		)
		{
			this.cluster = cluster;
			this.node = node;
			this.listener = listener;
			this.eventArgs = args;
			this.eventArgs.UserToken = this;
			this.sessionToken = node.SessionToken;
			this.dataBuffer = (this.sessionToken != null) ? new byte[256] : null; ;

			this.watch = Stopwatch.StartNew();
			AsyncTimeoutQueue.Instance.Add(this, cluster.connectionTimeout);

			node.IncrAsyncConnTotal();
			conn = new AsyncConnection(node.address, node);

			try
			{
				eventArgs.SetBuffer(dataBuffer, 0, 0);

				if (!conn.ConnectAsync(eventArgs))
				{
					ConnectionCreated();
				}
			}
			catch (Exception)
			{
				node.CloseAsyncConnOnError(conn);
				throw;
			}
		}

		internal void ConnectionCreated()
		{
			if (sessionToken != null)
			{
				AdminCommand command = new AdminCommand(dataBuffer, 0);
				dataLength = command.SetAuthenticate(cluster, sessionToken);
				eventArgs.SetBuffer(dataBuffer, 0, dataLength);
				dataOffset = 0;
				Send();
				return;
			}
			ConnectionReady();
		}

		private void Send()
		{
			if (!conn.SendAsync(eventArgs))
			{
				SendEvent();
			}
		}

		internal void SendEvent()
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

		private void ReceiveBegin()
		{
			dataOffset = 0;
			dataLength = dataOffset + 8;
			eventArgs.SetBuffer(dataOffset, 8);
			Receive();
		}

		private void Receive()
		{
			if (!conn.ReceiveAsync(eventArgs))
			{
				ReceiveEvent();
			}
		}

		internal void ReceiveEvent()
		{
			if (eventArgs.BytesTransferred <= 0)
			{
				Fail("Connection closed");
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
				long proto = ByteUtil.BytesToLong(dataBuffer, dataOffset);
				int length = (int)(proto & 0xFFFFFFFFFFFFL);

				if (length <= 0)
				{
					ReceiveBegin();
					return;
				}

				inHeader = false;

				if (length > dataBuffer.Length)
				{
					Fail("Invalid auth response");
					return;
				}
				eventArgs.SetBuffer(dataOffset, length);
				dataLength = dataOffset + length;
				Receive();
			}
			else
			{
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
					Fail("Failed to authenticate: " + resultCode);
					return;
				}
				ConnectionReady();
			}
		}

		public bool CheckTimeout()
		{
			if (state != 0)
			{
				return false; // Do not put back on timeout queue.
			}

			long elapsed = watch.ElapsedMilliseconds;

			if (elapsed < cluster.connectionTimeout)
			{
				return true; // Timeout not reached.
			}

			if (Interlocked.CompareExchange(ref state, 1, 0) == 0)
			{
				// Close connection. This will result in a socket error.
				if (conn != null)
				{
					node.CloseAsyncConnOnError(conn);
				}
			}
			return false; // Do not put back on timeout queue.
		}

		internal void Fail(string msg)
		{
			if (Interlocked.CompareExchange(ref state, 1, 0) == 0)
			{
				if (conn != null)
				{
					node.CloseAsyncConnOnError(conn);
					conn = null;
				}

				try
				{
					listener.OnFailure(msg);
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("OnFailure() error: " + Util.GetErrorMessage(e));
					}
				}
			}
		}

		private void ConnectionReady()
		{
			if (Interlocked.CompareExchange(ref state, 1, 0) == 0)
			{
				conn.UpdateLastUsed();
				node.PutAsyncConnection(conn);

				try
				{
					listener.OnSuccess(eventArgs);
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("OnSuccess() error: " + Util.GetErrorMessage(e));
					}
				}
			}
		}
	}

	public sealed class ConnectorHandlers
	{
		private static readonly ConnectorHandlers Instance = new ConnectorHandlers();
		public static readonly EventHandler<SocketAsyncEventArgs> SocketHandler = Instance.HandleSocketEvent;

		private void HandleSocketEvent(object sender, SocketAsyncEventArgs args)
		{
			AsyncConnector ac = args.UserToken as AsyncConnector;
			
			if (args.SocketError != SocketError.Success)
			{
				ac.Fail("Async min connections failed: " + args.SocketError);
				return;
			}
			
			try
			{
				switch (args.LastOperation)
				{
					case SocketAsyncOperation.Receive:
						ac.ReceiveEvent();
						break;
					case SocketAsyncOperation.Send:
						ac.SendEvent();
						break;
					case SocketAsyncOperation.Connect:
						ac.ConnectionCreated();
						break;
					default:
						ac.Fail("Invalid socket operation: " + args.LastOperation);
						break;
				}
			}
			catch (Exception e)
			{
				ac.Fail(e.Message);
			}
		}
	}

	public interface ConnectorListener
	{
		void OnSuccess(SocketAsyncEventArgs args);
		void OnFailure(string error);
	}
}
