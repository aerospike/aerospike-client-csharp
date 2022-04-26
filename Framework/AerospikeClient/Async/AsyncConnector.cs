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
					new AsyncConnector(cluster, node, this);
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

		public void OnSuccess()
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
						new AsyncConnector(cluster, node, this);
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

	public sealed class AsyncConnector : IAsyncCommand, ITimeout
	{		
		private readonly AsyncCluster cluster;
		private readonly AsyncNode node;
		private readonly ConnectorListener listener;
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
			ConnectorListener listener
		)
		{
			this.cluster = cluster;
			this.node = node;
			this.listener = listener;

			if (cluster.authEnabled)
			{
				this.sessionToken = node.SessionToken;
				this.dataBuffer = (this.sessionToken != null) ? new byte[256] : null;
			}
			else
			{
				this.sessionToken = null;
				this.dataBuffer = null;
			}

			this.watch = Stopwatch.StartNew();
			AsyncTimeoutQueue.Instance.Add(this, cluster.connectionTimeout);

			node.IncrAsyncConnTotal();
			conn = new AsyncConnection(node, this);

			try
			{
				conn.Connect();
			}
			catch (Exception)
			{
				node.CloseAsyncConnOnError(conn);
				throw;
			}
		}

		public void OnConnected()
		{
			if (sessionToken != null)
			{
				AdminCommand command = new AdminCommand(dataBuffer, 0);
				dataLength = command.SetAuthenticate(cluster, sessionToken);
				dataOffset = 0;
				conn.Send(dataBuffer, 0, dataLength);
				return;
			}
			ConnectionReady();
		}

		public void SendComplete()
		{
			conn.Receive(0, 8);
		}

		public void ReceiveComplete()
		{
			dataOffset = 0;

			if (inHeader)
			{
				long proto = ByteUtil.BytesToLong(dataBuffer, dataOffset);
				int length = (int)(proto & 0xFFFFFFFFFFFFL);

				if (length <= 0)
				{
					conn.Receive(dataOffset, 8);
					return;
				}

				inHeader = false;

				if (length > dataBuffer.Length)
				{
					Fail("Invalid auth response");
					return;
				}

				dataLength = dataOffset + length;
				conn.Receive(dataOffset, length);
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

		public void SocketFailed(SocketError se)
		{
			Fail("Create connection failed: " + se);
		}

		public void ConnectionFailed(AerospikeException ae)
		{
			OnAerospikeException(ae);
		}

		public void FailOnApplicationError(AerospikeException ae)
		{
			OnAerospikeException(ae);
		}

		public void OnAerospikeException(AerospikeException ae)
		{
			Fail("Create connection failed: " + ae.Message);
		}

		private void Fail(string msg)
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
					listener.OnSuccess();
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

	public interface ConnectorListener
	{
		void OnSuccess();
		void OnFailure(string error);
	}
}
