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
	/// Async connection base class.
	/// </summary>
	public abstract class AsyncConnection
	{
		private static readonly bool ZeroBuffers = !(
			Environment.OSVersion.Platform == PlatformID.Unix ||
			Environment.OSVersion.Platform == PlatformID.MacOSX);

		protected readonly Socket socket;
		protected IAsyncCommand command;
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

				lastUsed = DateTime.UtcNow;
			}
			catch (Exception e)
			{
				InitError(node);
				throw new AerospikeException.Connection(e);
			}
		}

		protected void InitError(AsyncNode node)
		{
			node.DecrAsyncConnTotal();
			node.IncrAsyncConnClosed();
			node.IncrErrorCount();
			socket.Dispose();
		}

		public IAsyncCommand Command
		{
			get {return command;}
			set {command = value;}
		}

		public abstract void Connect(IPEndPoint address);
		public abstract void Send(byte[] buffer, int offset, int count);
		public abstract void Receive(byte[] buffer, int offset, int count);

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

		public virtual void Reset()
		{
			command = null;
		}

		public virtual void Close()
		{
			try
			{
				socket.Shutdown(SocketShutdown.Both);
			}
			catch (Exception)
			{
			}

			try
			{
				socket.Dispose();
			}
			catch (Exception)
			{
			}
		}
	}

	public interface IAsyncCommand
	{
		void OnConnected();
		void SendComplete();
		void ReceiveComplete();
		void OnSocketError(SocketError se);
		void OnError(Exception e);
	}
}
