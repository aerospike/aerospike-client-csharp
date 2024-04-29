/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Net;
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Socket connection wrapper.
	/// </summary>
	public class Connection : IConnection
	{
		protected internal readonly Socket socket;
		protected internal readonly Pool<Connection> pool;
		private DateTime lastUsed;

		/// <summary>
		/// Create socket with connection timeout.
		/// </summary>
		public Connection(IPEndPoint address, int timeoutMillis, Pool<Connection> pool)
			: this(address, timeoutMillis, null, pool)
		{ 
		
		}

		/// <summary>
		/// Create socket with connection timeout.
		/// </summary>
		public Connection(IPEndPoint address, int timeoutMillis, Node node, Pool<Connection> pool)
		{
			this.pool = pool;

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
			}
			catch (Exception e)
			{
				throw new AerospikeException.Connection(e);
			}

			try
			{
				socket.NoDelay = true;

				if (timeoutMillis > 0)
				{
					socket.SendTimeout = timeoutMillis;
					socket.ReceiveTimeout = timeoutMillis;
				}
				else
				{
					// Never allow timeoutMillis of zero (no timeout) because WaitOne returns 
					// immediately when that happens!
					// Retry functionality will attempt to reconnect later.
					timeoutMillis = 2000;
				}

#if NETFRAMEWORK
				IAsyncResult result = socket.BeginConnect(address, null, null);
				WaitHandle wait = result.AsyncWaitHandle;

				if (wait.WaitOne(timeoutMillis))
				{
					// Connection succeeded.
					// EndConnect will automatically close AsyncWaitHandle.
					socket.EndConnect(result);
				}
				else
				{
					// Connection timed out.
					// Do not close AsyncWaitHandle. If AsyncWaitHandle is closed,
					// the disposed handle can be referenced after the timeout exception is thrown.
					// The handle will eventually get closed by the garbage collector.
					// See: https://social.msdn.microsoft.com/Forums/en-US/313cf28c-2a6d-498e-8188-7a0639dbd552/tcpclientbeginconnect-issue?forum=netfxnetcom
					throw new SocketException((int)SocketError.TimedOut);
				}
#else
				System.Threading.Tasks.Task task = socket.ConnectAsync(address);

				if (!task.Wait(timeoutMillis))
				{
					// Connection timed out.
					throw new SocketException((int)SocketError.TimedOut);
				}
#endif
				lastUsed = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				//socket.Close();
				socket.Dispose();

				if (node != null)
				{
					node.IncrErrorRate();
				}
				throw new AerospikeException.Connection(e);
			}
		}

		public void SetTimeout(int timeoutMillis)
		{
			socket.SendTimeout = timeoutMillis;
			socket.ReceiveTimeout = timeoutMillis;
		}

		public virtual void Write(byte[] buffer, int length)
		{
			int pos = 0;

			while (pos < length)
			{
				int count = socket.Send(buffer, pos, length - pos, SocketFlags.None);

				if (count <= 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				pos += count;
			}
		}

		public virtual void ReadFully(byte[] buffer, int length)
		{
			if (socket.ReceiveTimeout > 0)
			{
				// Check if data is available for reading.
				// Poll is used because the timeout value is respected under 500ms.
				// The Receive method does not timeout until after 500ms.
				if (!socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
				{
					throw new SocketException((int)SocketError.TimedOut);
				}
			}

			int pos = 0;

			while (pos < length)
			{
				int count = socket.Receive(buffer, pos, length - pos, SocketFlags.None);

				if (count <= 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				pos += count;
			}
		}

		public virtual Stream GetStream()
		{
			return new NetworkStream(socket);
		}

		/// <summary>
		/// Is socket closed from client perspective only.
		/// </summary>
		public bool IsClosed()
		{
			return !socket.Connected;
		}

		public DateTime LastUsed
		{
			get { return lastUsed; }
		}

		public void UpdateLastUsed()
		{
			this.lastUsed = DateTime.UtcNow;
		}

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public virtual void Close()
		{
			try
			{
				socket.Shutdown(SocketShutdown.Both);
			}
			catch (Exception)
			{
			}
			//socket.Close();
			socket.Dispose();
		}

		/// <summary>
		/// GetHostAddresses with timeout.
		/// </summary>
		/// <param name="host">Host name.</param>
		/// <param name="timeoutMillis">Timeout in milliseconds</param>
		public static IPAddress[] GetHostAddresses(string host, int timeoutMillis)
		{
#if NETFRAMEWORK
			IAsyncResult result = Dns.BeginGetHostAddresses(host, null, null);
			WaitHandle wait = result.AsyncWaitHandle;

			if (wait.WaitOne(timeoutMillis))
			{
				// EndGetHostAddresses will automatically close AsyncWaitHandle.
				IPAddress[] addresses = Dns.EndGetHostAddresses(result);

				if (addresses.Length == 0)
				{
					throw new AerospikeException.Connection("Failed to find addresses for " + host);
				}
				return addresses;
			}
			else
			{
				// Do not close AsyncWaitHandle because the disposed handle can be referenced after 
				// the exception is thrown. The handle will eventually get closed by the garbage collector.
				// See: https://social.msdn.microsoft.com/Forums/en-US/313cf28c-2a6d-498e-8188-7a0639dbd552/tcpclientbeginconnect-issue?forum=netfxnetcom
				throw new AerospikeException.Connection("Failed to resolve " + host);
			}
#else
			System.Threading.Tasks.Task<IPAddress[]> task = Dns.GetHostAddressesAsync(host);

			if (task.Wait(timeoutMillis))
			{
				IPAddress[] addresses = task.Result;

				if (addresses.Length == 0)
				{
					throw new AerospikeException.Connection("Failed to find addresses for " + host);
				}
				return addresses;
			}
			else
			{
				throw new AerospikeException.Connection("Failed to resolve " + host);
			}
#endif
		}
	}
}
