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
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous socket channel connection wrapper.
	/// </summary>
	public sealed class AsyncConnection
	{
		private readonly Socket socket;
		private readonly double maxSocketIdleMillis;
		private DateTime timestamp;

		public AsyncConnection(IPEndPoint address, AsyncCluster cluster)
		{
			this.maxSocketIdleMillis = (double)(cluster.maxSocketIdleMillis);

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				socket.NoDelay = true;

				// Docs say Blocking flag is ignored for async operations.
				// socket.Blocking = false;

				// Avoid internal TCP send/receive buffers.
				// Use application buffers directly.
				socket.SendBufferSize = 0;
				socket.ReceiveBufferSize = 0;

				timestamp = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				throw new AerospikeException.Connection(e);
			}
		}

		public bool ConnectAsync(SocketAsyncEventArgs args)
		{
			return socket.ConnectAsync(args);
		}

		public bool SendAsync(SocketAsyncEventArgs args)
		{
			return socket.SendAsync(args);
		}

		public bool ReceiveAsync(SocketAsyncEventArgs args)
		{
			return socket.ReceiveAsync(args);
		}

		public bool IsConnected()
		{
			return socket.Connected;
		}

		/// <summary>
		/// Is socket connected and used within specified limits.
		/// </summary>
		public bool IsValid()
		{
			return socket.Connected && (DateTime.UtcNow.Subtract(timestamp).TotalMilliseconds <= maxSocketIdleMillis);
		}

		public void UpdateLastUsed()
		{
			this.timestamp = DateTime.UtcNow;
		}

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public void Close()
		{
			try
			{
				socket.Shutdown(SocketShutdown.Both);
			}
			catch (Exception)
			{
			}
			socket.Close();
		}
	}
}
