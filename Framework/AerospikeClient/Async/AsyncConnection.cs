/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
#if !NETFRAMEWORK
using System.Runtime.InteropServices;
#endif

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous socket channel connection wrapper.
	/// </summary>
	public sealed class AsyncConnection
	{
		private readonly Socket socket;
		private readonly AsyncNode node;
		private readonly double maxSocketIdleMillis;
		private DateTime timestamp;

		public AsyncConnection(IPEndPoint address, AsyncCluster cluster, AsyncNode node)
		{
			this.maxSocketIdleMillis = (double)(cluster.maxSocketIdleMillis);
			this.node = node;

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				socket.NoDelay = true;

				// Docs say Blocking flag is ignored for async operations.
				// socket.Blocking = false;

				// Avoid internal TCP send/receive buffers.
				// Use application buffers directly.
#if !NETFRAMEWORK
				if (!RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
				{
					socket.SendBufferSize = 0;
					socket.ReceiveBufferSize = 0;
				}
#else
				socket.SendBufferSize = 0;
				socket.ReceiveBufferSize = 0;
#endif

				timestamp = DateTime.UtcNow;
				this.node.AddConnection();
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
			
			// Poll is much more accurate because sockets reaped by the server or sockets
			// that have unread data are identified. The problem is Poll decreases overall
			// benchmark performance by 10%.  Therefore, we will have to relay on retry 
			// mechanism to handle invalid sockets instead.
			//
			// Return true if socket is connected and has no data in it's buffer.
			// Return false, if not connected, socket read error or has data in it's buffer.
			/*
			try
			{
				return !socket.Poll(0, SelectMode.SelectRead);
			}
			catch (Exception)
			{
				return false;
			}*/
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
			node.DropConnection();

			try
			{
				socket.Shutdown(SocketShutdown.Both);
			}
			catch (Exception)
			{
			}
			socket.Dispose();
		}
	}
}
