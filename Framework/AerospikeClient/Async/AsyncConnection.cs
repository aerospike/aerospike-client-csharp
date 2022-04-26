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
using System.Net;
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous socket channel connection wrapper.
	/// </summary>
	public sealed class AsyncConnection
	{
		private static EventHandler<SocketAsyncEventArgs> SocketListener { get {return EventHandlers.SocketHandler;} }

		private static readonly bool ZeroBuffers = !(
			Environment.OSVersion.Platform == PlatformID.Unix ||
			Environment.OSVersion.Platform == PlatformID.MacOSX);

		private readonly Socket socket;
		private readonly SocketAsyncEventArgs args;
		private IAsyncCommand command;
		private DateTime lastUsed;

		public AsyncConnection(AsyncNode node, IAsyncCommand command)
		{
			this.command = command;

			IPEndPoint address = node.address;

			try
			{
				socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
			}
			catch (Exception e)
			{
				node.DecrAsyncConnTotal();
				node.IncrErrorCount();
				throw new AerospikeException.Connection(e);
			}

			node.IncrAsyncConnOpened();

			try
			{
				socket.NoDelay = true;

				// Docs say Blocking flag is ignored for async operations.
				// socket.Blocking = false;

				if (ZeroBuffers)
				{
					// Avoid internal TCP send/receive buffers.
					// Use application buffers directly.
					socket.SendBufferSize = 0;
					socket.ReceiveBufferSize = 0;
				}

				args = new SocketAsyncEventArgs();
				args.UserToken = this;
				args.RemoteEndPoint = address;
				args.Completed += SocketListener;

				lastUsed = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				socket.Dispose();
				node.DecrAsyncConnTotal();
				node.IncrAsyncConnClosed();
				node.IncrErrorCount();
				throw new AerospikeException.Connection(e);
			}
		}

		public IAsyncCommand Command
		{
			get {return command;}
			set {command = value;}
		}

		public void Connect()
		{
			if (!socket.ConnectAsync(args))
			{
				command.OnConnected();
			}
		}

		public void Send(byte[] buffer, int offset, int count)
		{
			args.SetBuffer(buffer, offset, count);
			Send();
		}

		private void Send()
		{
			if (!socket.SendAsync(args))
			{
				SendEvent();
			}
		}

		private void SendEvent()
		{
			int sent = args.BytesTransferred;

			if (sent <= 0)
			{
				// When a node has shutdown on linux, async command send events return zero
				// with SocketError.Success. If zero bytes sent on send, cancel command.
				command.OnError(new AerospikeException.Connection("Connection closed"));
				return;
			}

			if (sent < args.Count)
			{
				args.SetBuffer(args.Offset + sent, args.Count - sent);
				Send();
				return;
			}

			command.SendComplete();
		}

		public void Receive(byte[] buffer, int offset, int count)
		{
			args.SetBuffer(buffer, offset, count);
			Receive();
		}

		public void Receive(int offset, int count)
		{
			args.SetBuffer(offset, count);
			Receive();
		}

		private void Receive()
		{
			if (!socket.ReceiveAsync(args))
			{
				ReceiveEvent();
			}
		}

		private void ReceiveEvent()
		{
			int received = args.BytesTransferred;

			if (received <= 0)
			{
				command.OnError(new AerospikeException.Connection("Connection closed"));
				return;
			}

			if (received < args.Count)
			{
				args.SetBuffer(args.Offset + received, args.Count - received);
				Receive();
				return;
			}

			command.ReceiveComplete();
		}

		/// <summary>
		/// Is socket connected and used within specified limits.
		/// </summary>
		public bool IsValid()
		{
			return socket.Connected;
			
			// Poll is much more accurate because sockets reaped by the server or sockets
			// that have unread data are identified. The problem is Poll decreases overall
			// benchmark performance by 10%.  Therefore, we will have to rely on retry 
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

		public DateTime LastUsed
		{
			get { return lastUsed; }
		}

		public void UpdateLastUsed()
		{
			this.lastUsed = DateTime.UtcNow;
		}

		public void Reset()
		{
			args.SetBuffer(null, 0, 0);
			command = null;
		}

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public void Close()
		{
			try
			{
				command = null;
				socket.Shutdown(SocketShutdown.Both);
			}
			catch (Exception)
			{
			}

			try
			{
				socket.Dispose();
				args.Dispose();
			}
			catch (Exception)
			{
			}
		}

		// Wrap the stateless event handlers in an instance, in order to avoid static delegate performance penalty.
		private sealed class EventHandlers
		{
			private static readonly EventHandlers Instance = new EventHandlers();
			public static readonly EventHandler<SocketAsyncEventArgs> SocketHandler = Instance.HandleSocketEvent;

			private void HandleSocketEvent(object sender, SocketAsyncEventArgs args)
			{
				AsyncConnection conn = args.UserToken as AsyncConnection;

				if (args.SocketError != SocketError.Success)
				{
					SocketFailed(conn, args.SocketError);
					return;
				}

				try
				{
					switch (args.LastOperation)
					{
						case SocketAsyncOperation.Receive:
							conn.ReceiveEvent();
							break;
						case SocketAsyncOperation.Send:
							conn.SendEvent();
							break;
						case SocketAsyncOperation.Connect:
							conn.command.OnConnected();
							break;
						default:
							throw new AerospikeException("Invalid socket operation: " + args.LastOperation);
					}
				}
				catch (Exception e)
				{
					try
					{
						conn.command.OnError(e);
					}
					catch (Exception ne)
					{
						Log.Error("OnError failed: " + Util.GetErrorMessage(ne) +
							System.Environment.NewLine + "Original error: " + Util.GetErrorMessage(e));
					}
				}
			}

			private static void SocketFailed(AsyncConnection conn, SocketError se)
			{
				try
				{
					conn.command.SocketFailed(se);
				}
				catch (Exception ne)
				{
					Log.Error("SocketFailed failed: " + Util.GetErrorMessage(ne) +
						System.Environment.NewLine + "Original error: " + se);
				}
			}
		}
	}

	public interface IAsyncCommand
	{
		void OnConnected();
		void SendComplete();
		void ReceiveComplete();
		void SocketFailed(SocketError se);
		void OnError(Exception e);
	}
}
