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
	/// Normal async connection implemented with SocketAsyncEventArgs.
	/// </summary>
	public sealed class AsyncConnectionArgs : AsyncConnection
	{
		private readonly SocketAsyncEventArgs args;

		public AsyncConnectionArgs(AsyncNode node, IAsyncCommand command)
			: base(node, command)
		{
			try
			{
				args = new SocketAsyncEventArgs();
				args.Completed += AsyncCompleted;
			}
			catch (Exception e)
			{
				base.InitError(node);
				throw new AerospikeException.Connection(e);
			}
		}

		private void AsyncCompleted(object sender, SocketAsyncEventArgs args)
		{
			try
			{
				switch (args.LastOperation)
				{
					case SocketAsyncOperation.Connect:
						ConnectEvent(args);
						break;
					case SocketAsyncOperation.Send:
						SendEvent(args);
						break;
					case SocketAsyncOperation.Receive:
						ReceiveEvent(args);
						break;
					default:
						throw new AerospikeException("Invalid socket operation: " + args.LastOperation);
				}
			}
			catch (Exception e)
			{
				if (command == null)
				{
					Log.Error(node.cluster.context, "Received async event when connection is in pool.");
					return;
				}

				try
				{
					command.OnError(e);
				}
				catch (Exception ne)
				{
					Log.Error(node.cluster.context, "OnError failed: " + Util.GetErrorMessage(ne) +
						System.Environment.NewLine + "Original error: " + Util.GetErrorMessage(e));
				}
			}
		}

		public override void Connect(IPEndPoint address)
		{
			args.RemoteEndPoint = address;

			if (!socket.ConnectAsync(args))
			{
				ConnectEvent(args);
			}
		}

		private void ConnectEvent(SocketAsyncEventArgs args)
		{
			if (args.SocketError == SocketError.Success)
			{
				command.OnConnected();
			}
			else
			{
				command.OnSocketError(args.SocketError);
			}
		}

		public override void Send(byte[] buffer, int offset, int count)
		{
			args.SetBuffer(buffer, offset, count);
			Send();
		}

		private void Send()
		{
			if (!socket.SendAsync(args))
			{
				SendEvent(args);
			}
		}

		private void SendEvent(SocketAsyncEventArgs args)
		{
			if (args.SocketError == SocketError.Success)
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
			else
			{
				command.OnSocketError(args.SocketError);
			}
		}

		public override void Receive(byte[] buffer, int offset, int count)
		{
			args.SetBuffer(buffer, offset, count);
			Receive();
		}

		private void Receive()
		{
			if (!socket.ReceiveAsync(args))
			{
				ReceiveEvent(args);
			}
		}

		private void ReceiveEvent(SocketAsyncEventArgs args)
		{
			if (args.SocketError == SocketError.Success)
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
			else
			{
				command.OnSocketError(args.SocketError);
			}
		}

		public override void Reset()
		{
			command = null;
			args.SetBuffer(null, 0, 0);
		}

		public override void Close()
		{
			base.Close();

			try
			{
				args.Dispose();
			}
			catch (Exception)
			{
			}
		}
	}
}
