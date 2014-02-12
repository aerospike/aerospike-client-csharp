/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
			this.maxSocketIdleMillis = (double)(cluster.maxSocketIdle * 1000);

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
