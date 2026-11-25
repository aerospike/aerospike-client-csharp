/*
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Stub Aerospike node for the L5 driver tests: answers info requests on 127.0.0.1, in the clear
	/// or over TLS, from <see cref="Values"/>, and records every info command it receives. A command
	/// it has no value for gets <c>ok</c>; any other message closes the connection. The partition
	/// generation is 1 and the partition map is empty.
	/// </summary>
	internal sealed class StubInfoNode : IDisposable
	{
		internal readonly ConcurrentDictionary<string, string> Values =
			new ConcurrentDictionary<string, string>(StringComparer.Ordinal);

		private readonly ConcurrentQueue<string> commands = new ConcurrentQueue<string>();
		private readonly ConcurrentQueue<Socket> sockets = new ConcurrentQueue<Socket>();
		private readonly TcpListener listener;
		private readonly X509Certificate2 serverCert;
		private readonly CancellationTokenSource cancel = new CancellationTokenSource();

		internal StubInfoNode(string name, X509Certificate2 tlsCert = null)
		{
			serverCert = tlsCert;
			listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			Values["node"] = name;
			Values["partition-generation"] = "1";
			Values["replicas"] = "";
			Values["peers-generation"] = "1";
			Values["build"] = "8.1.0.0";

			Thread thread = new Thread(Accept) { IsBackground = true, Name = "stub-" + name };
			thread.Start();
		}

		internal int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

		internal List<string> Commands() => commands.ToList();

		private void Accept()
		{
			while (!cancel.IsCancellationRequested)
			{
				Socket socket;

				try
				{
					socket = listener.AcceptSocket();
				}
				catch (SocketException)
				{
					return;
				}
				catch (ObjectDisposedException)
				{
					return;
				}

				sockets.Enqueue(socket);
				Thread thread = new Thread(() => Serve(socket)) { IsBackground = true };
				thread.Start();
			}
		}

		private void Serve(Socket socket)
		{
			try
			{
				using (socket)
				{
					Stream stream = new NetworkStream(socket, true);

					if (serverCert != null)
					{
						SslStream ssl = new SslStream(stream, false);
						ssl.AuthenticateAsServer(serverCert, false, SslProtocols.Tls12, false);
						stream = ssl;
					}

					byte[] header = new byte[8];

					while (true)
					{
						ReadFully(stream, header, 8);

						if (header[1] != 1)
						{
							return;
						}

						long size = ByteUtil.BytesToLong(header, 0) & 0xFFFFFFFFFFFFL;
						byte[] request = new byte[(int)size];
						ReadFully(stream, request, request.Length);
						StringBuilder sb = new StringBuilder();

						foreach (string command in Encoding.UTF8.GetString(request).Split('\n'))
						{
							if (command.Length == 0)
							{
								continue;
							}

							commands.Enqueue(command);
							Values.TryGetValue(command, out string value);
							sb.Append(command).Append('\t').Append(value ?? "ok").Append('\n');
						}

						byte[] body = Encoding.UTF8.GetBytes(sb.ToString());
						byte[] response = new byte[8 + body.Length];
						ulong proto = ((ulong)body.Length) | (2UL << 56) | (1UL << 48);
						ByteUtil.LongToBytes(proto, response, 0);
						Buffer.BlockCopy(body, 0, response, 8, body.Length);
						stream.Write(response, 0, response.Length);
						stream.Flush();
					}
				}
			}
			catch (IOException)
			{
			}
			catch (SocketException)
			{
			}
			catch (ObjectDisposedException)
			{
			}
		}

		private static void ReadFully(Stream stream, byte[] buffer, int length)
		{
			int pos = 0;

			while (pos < length)
			{
				int n = stream.Read(buffer, pos, length - pos);

				if (n <= 0)
				{
					throw new EndOfStreamException();
				}

				pos += n;
			}
		}

		public void Dispose()
		{
			cancel.Cancel();

			try
			{
				listener.Stop();
			}
			catch (SocketException)
			{
			}

			foreach (Socket socket in sockets)
			{
				try
				{
					socket.Close();
				}
				catch (SocketException)
				{
				}
			}

			cancel.Dispose();
		}
	}
}
