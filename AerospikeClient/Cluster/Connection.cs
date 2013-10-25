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
			: this(address, timeoutMillis, 14)
		{
		}

		/// <summary>
		/// Create socket with connection timeout.
		/// </summary>
		public Connection(IPEndPoint address, int timeoutMillis, int maxSocketIdleSeconds)
		{
			this.maxSocketIdleMillis = (double)(maxSocketIdleSeconds * 1000);

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				socket.NoDelay = true;
				socket.SendTimeout = timeoutMillis;
				socket.ReceiveTimeout = timeoutMillis;

				IAsyncResult result = socket.BeginConnect(address, null, null);
				WaitHandle wait = result.AsyncWaitHandle;

				try
				{
					if (wait.WaitOne(timeoutMillis))
					{
						socket.EndConnect(result);
					}
					else
					{
						socket.Close();
						throw new SocketException((int)SocketError.TimedOut);
					}
				}
				finally
				{
					wait.Close();
				}
				timestamp = DateTime.Now;
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

		public bool IsConnected()
		{
			return socket.Connected;
		}

		/// <summary>
		/// Is socket connected and used within specified limits.
		/// </summary>
		public bool IsValid()
		{
			return socket.Connected && ((DateTime.Now - timestamp).TotalMilliseconds <= maxSocketIdleMillis);
		}

		public void UpdateLastUsed()
		{
			this.timestamp = DateTime.Now;
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

			try
			{
				if (wait.WaitOne(timeoutMillis))
				{
					IPAddress[] addresses = Dns.EndGetHostAddresses(result);

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
			}
			finally
			{
				wait.Close();
			}
		}

		public Socket Socket
		{
			get { return socket; }
		}
	}
}