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
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace Aerospike.Client
{
	/// <summary>
	/// Async TLS connection.
	/// </summary>
	public sealed class AsyncConnectionTls : AsyncConnection
	{
		private readonly AsyncNode node;
		private readonly AsyncCallback writeHandler;
		private readonly AsyncCallback readHandler;
		private SslStream sslStream;
		private byte[] buffer;
		private int offset;
		private int count;

		public AsyncConnectionTls(AsyncNode node, IAsyncCommand command)
			: base(node, command)
		{
			this.node = node;
			this.writeHandler = SendEvent;
			this.readHandler = ReceiveEvent;
		}

		public override void Connect(IPEndPoint address)
		{
			SocketAsyncEventArgs args = new SocketAsyncEventArgs();
			args.RemoteEndPoint = address;
			args.Completed += ConnectComplete;

			if (!socket.ConnectAsync(args))
			{
				ConnectEvent(args);
			}
		}

		private void ConnectComplete(object sender, SocketAsyncEventArgs args)
		{
			try
			{
				if (args.LastOperation == SocketAsyncOperation.Connect)
				{
					ConnectEvent(args);
				}
				else
				{
					throw new AerospikeException("Invalid socket operation: " + args.LastOperation);
				}
			}
			catch (Exception e)
			{
				if (command == null)
				{
					Log.Error("Received async event when connection is in pool.");
					return;
				}

				try
				{
					command.OnError(e);
				}
				catch (Exception ne)
				{
					Log.Error("OnError failed: " + Util.GetErrorMessage(ne) +
						System.Environment.NewLine + "Original error: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void ConnectEvent(SocketAsyncEventArgs args)
		{
			try
			{
				if (args.SocketError == SocketError.Success)
				{
					sslStream = new SslStream(new NetworkStream(socket, true), false, ValidateServerCertificate);
					TlsPolicy policy = node.cluster.tlsPolicy;
					sslStream.BeginAuthenticateAsClient(node.host.tlsName, policy.clientCertificates, policy.protocols, false, HandshakeEvent, null);
				}
				else
				{
					command.OnSocketError(args.SocketError);
				}
			}
			finally
			{
				args.Dispose();
			}
		}

		private bool ValidateServerCertificate
		(
			object sender,
			X509Certificate cert,
			X509Chain chain,
			SslPolicyErrors sslPolicyErrors
		)
		{
			return TlsConnection.ValidateCertificate(node.cluster.tlsPolicy, node.host.tlsName, cert, sslPolicyErrors);
		}

		private void HandshakeEvent(IAsyncResult result)
		{
			try
			{
				sslStream.EndAuthenticateAsClient(result);
				command.OnConnected();
			}
			catch (Exception e)
			{
				command.OnError(e);
			}
		}

		public override void Send(byte[] buffer, int offset, int count)
		{
			sslStream.BeginWrite(buffer, offset, count, writeHandler, null);
		}

		private void SendEvent(IAsyncResult result)
		{
			try
			{
				sslStream.EndWrite(result);
				command.SendComplete();
			}
			catch (Exception e)
			{
				command.OnError(e);
			}
		}

		public override void Receive(byte[] buffer, int offset, int count)
		{
			this.buffer = buffer;
			this.offset = offset;
			this.count = count;
			sslStream.BeginRead(buffer, offset, count, readHandler, null);
		}

		private void ReceiveEvent(IAsyncResult result)
		{
			try
			{
				int received = sslStream.EndRead(result);

				if (received <= 0)
				{
					command.OnError(new AerospikeException.Connection("Connection closed"));
					return;
				}

				if (received < count)
				{
					offset += received;
					count -= received;
					sslStream.BeginRead(buffer, offset, count, readHandler, null);
					return;
				}

				command.ReceiveComplete();
			}
			catch (Exception e)
			{
				command.OnError(e);
			}
		}

		public override void Reset()
		{
			command = null;
			buffer = null;
		}

		public override void Close()
		{
			try
			{
				sslStream.Dispose();
			}
			catch (Exception)
			{
			}

			base.Close();
		}
	}
}
