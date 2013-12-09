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
	/// Asynchronous socket channel connection wrapper.
	/// </summary>
	public sealed class AsyncConnection
	{
		private readonly Socket socket;
		private readonly SocketAsyncEventArgs args;
		private readonly double maxSocketIdleMillis;
		private DateTime timestamp;

		public AsyncConnection(IPEndPoint address, AsyncCluster cluster, AsyncCommand command, EventHandler<SocketAsyncEventArgs> handler)
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

				args = new SocketAsyncEventArgs();
				args.Completed += handler;
				args.RemoteEndPoint = address;
				args.UserToken = command;
				timestamp = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				throw new AerospikeException.Connection(e);
			}
		}

		public SocketAsyncEventArgs SetCommand(AsyncCommand command)
		{
			args.UserToken = command;
			return args;
		}

		public bool ConnectAsync()
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

		public SocketAsyncEventArgs Args
		{
			get { return args; }
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
			args.Dispose();
		}
	}
}