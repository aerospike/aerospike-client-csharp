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
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Aerospike.Test
{
	/// <summary>
	/// Loopback server implementing only the Aerospike info-protocol subset the
	/// node validator uses: one framed request of newline-separated commands,
	/// answered with scripted <c>name\tvalue\n</c> lines. It is not a general
	/// Aerospike test server.
	/// </summary>
	internal sealed class MetricsInfoTestServer : IDisposable
	{
		internal enum Mode
		{
			/// <summary>Answer every info request from the script.</summary>
			Respond,

			/// <summary>Accept the transport and close it before any response.</summary>
			CloseOnAccept
		}

		private const ulong InfoHeader = (2UL << 56) | (1UL << 48);
		private const ulong LengthMask = 0xFFFFFFFFFFFFUL;

		private readonly TcpListener listener;
		private readonly Thread acceptThread;
		private readonly IReadOnlyDictionary<string, string> script;
		private readonly object sync = new();
		private readonly List<TcpClient> clients = new();
		private readonly List<Thread> handlers = new();
		private readonly List<string> requests = new();
		private volatile Mode mode;
		private int accepted;
		private bool disposed;

		internal MetricsInfoTestServer(
			IReadOnlyDictionary<string, string> script,
			Mode mode = Mode.Respond)
		{
			this.script = script;
			this.mode = mode;
			listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			Port = ((IPEndPoint)listener.LocalEndpoint).Port;
			acceptThread = new Thread(AcceptLoop)
			{
				IsBackground = true,
				Name = "metrics-info-test-server"
			};
			acceptThread.Start();
		}

		internal int Port { get; }

		internal Mode CurrentMode
		{
			get => mode;
			set => mode = value;
		}

		internal int AcceptedConnections => Volatile.Read(ref accepted);

		internal IReadOnlyList<string> Requests
		{
			get
			{
				lock (sync)
				{
					return requests.ToArray();
				}
			}
		}

		private void AcceptLoop()
		{
			while (true)
			{
				TcpClient client;

				try
				{
					client = listener.AcceptTcpClient();
				}
				catch (SocketException)
				{
					return;
				}
				catch (ObjectDisposedException)
				{
					return;
				}

				Interlocked.Increment(ref accepted);

				if (mode == Mode.CloseOnAccept)
				{
					client.Client.LingerState = new LingerOption(true, 0);
					client.Close();
					continue;
				}

				Thread handler = new(() => Serve(client))
				{
					IsBackground = true,
					Name = "metrics-info-test-connection"
				};

				lock (sync)
				{
					if (disposed)
					{
						client.Close();
						return;
					}
					clients.Add(client);
					handlers.Add(handler);
				}
				handler.Start();
			}
		}

		private void Serve(TcpClient client)
		{
			try
			{
				client.ReceiveTimeout = 30_000;
				client.SendTimeout = 30_000;
				NetworkStream stream = client.GetStream();
				byte[] header = new byte[8];

				while (ReadFully(stream, header))
				{
					ulong size = BinaryPrimitives.ReadUInt64BigEndian(header);
					byte[] payload = new byte[(int)(size & LengthMask)];

					if (!ReadFully(stream, payload))
					{
						return;
					}

					string request = Encoding.UTF8.GetString(payload);
					lock (sync)
					{
						requests.Add(request);
					}

					StringBuilder response = new();
					foreach (string command in request.Split(
						'\n',
						StringSplitOptions.RemoveEmptyEntries))
					{
						response.Append(command);
						if (script.TryGetValue(command, out string value))
						{
							response.Append('\t').Append(value);
						}
						response.Append('\n');
					}

					byte[] body = Encoding.UTF8.GetBytes(response.ToString());
					byte[] frame = new byte[8 + body.Length];
					BinaryPrimitives.WriteUInt64BigEndian(
						frame,
						InfoHeader | (uint)body.Length);
					body.CopyTo(frame, 8);
					stream.Write(frame, 0, frame.Length);
				}
			}
			catch (IOException)
			{
			}
			catch (ObjectDisposedException)
			{
			}
			finally
			{
				client.Close();
			}
		}

		private static bool ReadFully(NetworkStream stream, byte[] buffer)
		{
			int offset = 0;

			while (offset < buffer.Length)
			{
				int count = stream.Read(buffer, offset, buffer.Length - offset);

				if (count == 0)
				{
					return false;
				}
				offset += count;
			}
			return true;
		}

		public void Dispose()
		{
			TcpClient[] open;
			Thread[] running;

			lock (sync)
			{
				if (disposed)
				{
					return;
				}
				disposed = true;
				open = clients.ToArray();
				running = handlers.ToArray();
			}

			listener.Stop();
			acceptThread.Join();

			foreach (TcpClient client in open)
			{
				client.Close();
			}
			foreach (Thread handler in running)
			{
				handler.Join();
			}
		}
	}
}
