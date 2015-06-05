/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	/// Socket connection wrapper.
	/// </summary>
	public sealed class Connection
	{
		private readonly Socket socket;
		private readonly double maxSocketIdleMillis;
		private DateTime timestamp;

		public Connection(IPEndPoint address, int timeoutMillis)
			: this(address, timeoutMillis, 14000)
		{
		}

		/// <summary>
		/// Create socket with connection timeout.
		/// </summary>
		public Connection(IPEndPoint address, int timeoutMillis, int maxSocketIdleMillis)
		{
			this.maxSocketIdleMillis = (double)(maxSocketIdleMillis);

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				socket.NoDelay = true;

				if (timeoutMillis > 0)
				{
					socket.SendTimeout = timeoutMillis;
					socket.ReceiveTimeout = timeoutMillis;
				}
				else
				{
					// Do not wait indefinitely on connection if no timeout is specified.
					// Retry functionality will attempt to reconnect later.
					timeoutMillis = 2000;
				}

				IAsyncResult result = socket.BeginConnect(address, null, null);
				WaitHandle wait = result.AsyncWaitHandle;

				// Never allow timeoutMillis of zero because WaitOne returns 
				// immediately when that happens!
				if (wait.WaitOne(timeoutMillis))
				{
					// EndConnect will automatically close AsyncWaitHandle.
					socket.EndConnect(result);
				}
				else
				{
					// Close socket, but do not close AsyncWaitHandle. If AsyncWaitHandle is closed,
					// the disposed handle can be referenced after the timeout exception is thrown.
					// The handle will eventually get closed by the garbage collector.
					// See: https://social.msdn.microsoft.com/Forums/en-US/313cf28c-2a6d-498e-8188-7a0639dbd552/tcpclientbeginconnect-issue?forum=netfxnetcom
					socket.Close();
					throw new SocketException((int)SocketError.TimedOut);
				}
				timestamp = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				throw new AerospikeException.Connection(e);
			}
		}

		public void SetTimeout(int timeoutMillis)
		{
			socket.SendTimeout = timeoutMillis;
			socket.ReceiveTimeout = timeoutMillis;
		}

		public void Write(byte[] buffer, int length)
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

		public void ReadFully(byte[] buffer, int length)
		{
			if (socket.ReceiveTimeout > 0)
			{
				// Check if data is available for reading.
				// Poll is used because the timeout value is respected under 500ms.
				// The Receive method does not timeout until after 500ms.
				if (! socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
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

		/// <summary>
		/// GetHostAddresses with timeout.
		/// </summary>
		/// <param name="host">Host name.</param>
		/// <param name="timeoutMillis">Timeout in milliseconds</param>
		public static IPAddress[] GetHostAddresses(string host, int timeoutMillis)
		{
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
		}

		public Socket Socket
		{
			get { return socket; }
		}
	}
}
