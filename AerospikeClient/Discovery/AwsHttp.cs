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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Minimal HTTP/1.1 client with every connect, handshake and read bounded by one deadline. One
	/// connection per request, closed when the response is read, so no connection is pooled and no
	/// thread is started.
	/// </summary>
	internal static class AwsHttp
	{
		internal const int MaxBody = 16 * 1024 * 1024;

		internal sealed class Response
		{
			internal readonly int Status;
			internal readonly Dictionary<string, string> Headers;
			internal readonly byte[] Body;

			internal Response(int status, Dictionary<string, string> headers, byte[] body)
			{
				Status = status;
				Headers = headers;
				Body = body;
			}

			internal string Text()
			{
				return Encoding.UTF8.GetString(Body);
			}
		}

		/// <summary>
		/// Return a <see cref="Stopwatch"/>-based deadline for the given budget in milliseconds.
		/// </summary>
		internal static long Deadline(int budgetMs)
		{
			return Stopwatch.GetTimestamp() + budgetMs * Stopwatch.Frequency / 1000;
		}

		/// <summary>
		/// Return the Host header value for the URI.
		/// </summary>
		internal static string Host(Uri uri)
		{
			return uri.IsDefaultPort ? uri.Host : uri.Host + ':' + uri.Port;
		}

		/// <summary>
		/// Send one request and read the response. Header names must be lower case; Host is added when
		/// absent. <paramref name="deadline"/> is a <see cref="Stopwatch.GetTimestamp"/> value.
		/// </summary>
		/// <exception cref="TimeoutException">if the deadline passes</exception>
		/// <exception cref="IOException">on any other I/O or protocol failure</exception>
		internal static Response Request(
			string method,
			Uri uri,
			IDictionary<string, string> headers,
			byte[] body,
			long deadline)
		{
			bool tls = uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase);
			string host = uri.Host;
			int port = uri.IsDefaultPort ? (tls ? 443 : 80) : uri.Port;
			IPAddress[] addresses = Dns.GetHostAddresses(host);

			if (addresses.Length == 0)
			{
				throw new IOException("no addresses for " + host);
			}

			IPAddress address = null;

			foreach (IPAddress candidate in addresses)
			{
				if (candidate.AddressFamily == AddressFamily.InterNetwork)
				{
					address = candidate;
					break;
				}
			}

			address ??= addresses[0];

			using (Socket socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp))
			{
				socket.NoDelay = true;
				IAsyncResult connect = socket.BeginConnect(new IPEndPoint(address, port), null, null);

				if (!connect.AsyncWaitHandle.WaitOne(Remaining(deadline)))
				{
					try { socket.Close(); } catch (Exception) { }
					throw new TimeoutException("timed out");
				}

				socket.EndConnect(connect);
				socket.ReceiveTimeout = Remaining(deadline);
				socket.SendTimeout = Remaining(deadline);

				Stream stream = new NetworkStream(socket, ownsSocket: false);

				if (tls)
				{
					SslStream ssl = new SslStream(stream, false);
					ssl.AuthenticateAsClient(host);
					stream = ssl;
					socket.ReceiveTimeout = Remaining(deadline);
					socket.SendTimeout = Remaining(deadline);
				}

				try
				{
					string path = string.IsNullOrEmpty(uri.AbsolutePath) ? "/" : uri.AbsolutePath;

					if (!string.IsNullOrEmpty(uri.Query))
					{
						path += uri.Query;
					}

					StringBuilder sb = new StringBuilder(512);
					sb.Append(method).Append(' ').Append(path).Append(" HTTP/1.1\r\n");

					if (!headers.ContainsKey("host"))
					{
						sb.Append("host: ").Append(Host(uri)).Append("\r\n");
					}

					foreach (KeyValuePair<string, string> entry in headers)
					{
						sb.Append(entry.Key).Append(": ").Append(entry.Value).Append("\r\n");
					}

					if (body != null)
					{
						sb.Append("content-length: ").Append(body.Length).Append("\r\n");
					}

					sb.Append("connection: close\r\n\r\n");

					byte[] head = Encoding.UTF8.GetBytes(sb.ToString());
					socket.ReceiveTimeout = Remaining(deadline);
					socket.SendTimeout = Remaining(deadline);
					stream.Write(head, 0, head.Length);

					if (body != null)
					{
						stream.Write(body, 0, body.Length);
					}

					stream.Flush();
					return Read(new Reader(socket, stream, deadline));
				}
				finally
				{
					stream.Dispose();
				}
			}
		}

		private static Response Read(Reader reader)
		{
			string line = reader.Line();
			string[] fields = line.Split(' ', 3);

			if (fields.Length < 2 || !fields[0].StartsWith("HTTP/", StringComparison.Ordinal))
			{
				throw new ProtocolException("invalid HTTP status line");
			}

			if (!int.TryParse(fields[1], out int status))
			{
				throw new ProtocolException("invalid HTTP status line");
			}

			Dictionary<string, string> headers = new Dictionary<string, string>();

			while (!(line = reader.Line()).Equals(""))
			{
				int i = line.IndexOf(':');

				if (i > 0)
				{
					headers[line.Substring(0, i).Trim().ToLowerInvariant()] = line.Substring(i + 1).Trim();
				}
			}

			MemoryStream body = new MemoryStream();
			headers.TryGetValue("content-length", out string length);
			headers.TryGetValue("transfer-encoding", out string transfer);

			if (transfer != null && transfer.Equals("chunked", StringComparison.OrdinalIgnoreCase))
			{
				while (true)
				{
					string size = reader.Line();
					int semi = size.IndexOf(';');
					string hex = ((semi >= 0) ? size.Substring(0, semi) : size).Trim();

					if (!int.TryParse(hex, System.Globalization.NumberStyles.HexNumber, null, out int n))
					{
						throw new ProtocolException("invalid HTTP chunk size");
					}

					if (n == 0)
					{
						while (!reader.Line().Equals(""))
						{
						}
						break;
					}

					reader.Copy(body, n);
					reader.Line();
				}
			}
			else if (length != null)
			{
				if (!int.TryParse(length, out int n))
				{
					throw new ProtocolException("invalid HTTP content-length");
				}

				reader.Copy(body, n);
			}
			else if (status >= 200 && status != 204 && status != 304)
			{
				reader.Copy(body, -1);
			}

			return new Response(status, headers, body.ToArray());
		}

		/// <summary>
		/// Return the milliseconds left before the deadline, rounded up, for a socket timeout.
		/// </summary>
		/// <exception cref="TimeoutException">if the deadline has passed</exception>
		internal static int Remaining(long deadline)
		{
			long remaining = deadline - Stopwatch.GetTimestamp();

			if (remaining <= 0)
			{
				throw new TimeoutException("timed out");
			}

			long ms = (remaining * 1000 + Stopwatch.Frequency - 1) / Stopwatch.Frequency;
			return (int)Math.Min(int.MaxValue, ms);
		}

		private sealed class Reader
		{
			private readonly Socket socket;
			private readonly Stream stream;
			private readonly long deadline;
			private readonly byte[] buffer = new byte[8192];
			private int offset;
			private int limit;
			private int total;

			internal Reader(Socket socket, Stream stream, long deadline)
			{
				this.socket = socket;
				this.stream = stream;
				this.deadline = deadline;
			}

			private bool Fill()
			{
				socket.ReceiveTimeout = Remaining(deadline);
				int n;

				try
				{
					n = stream.Read(buffer, 0, buffer.Length);
				}
				catch (IOException e) when (IsTimeout(e))
				{
					throw new TimeoutException("timed out", e);
				}

				if (n <= 0)
				{
					return false;
				}

				total += n;

				if (total > MaxBody)
				{
					throw new ProtocolException("HTTP response exceeds " + MaxBody + " bytes");
				}

				offset = 0;
				limit = n;
				return true;
			}

			internal string Line()
			{
				MemoryStream sb = new MemoryStream(128);

				while (true)
				{
					if (offset == limit && !Fill())
					{
						throw new EndOfStreamException("HTTP connection closed");
					}

					byte b = buffer[offset++];

					if (b == (byte)'\n')
					{
						break;
					}

					if (b != (byte)'\r')
					{
						sb.WriteByte(b);
					}
				}

				return Encoding.GetEncoding("ISO-8859-1").GetString(sb.ToArray());
			}

			internal void Copy(MemoryStream output, int length)
			{
				while (length != 0)
				{
					if (offset == limit && !Fill())
					{
						if (length < 0)
						{
							return;
						}

						throw new EndOfStreamException("HTTP connection closed");
					}

					int n = limit - offset;

					if (length > 0 && n > length)
					{
						n = length;
					}

					output.Write(buffer, offset, n);
					offset += n;

					if (length > 0)
					{
						length -= n;
					}
				}
			}

			private static bool IsTimeout(Exception e)
			{
				return e is SocketException se && se.SocketErrorCode == SocketError.TimedOut
					|| e.InnerException is SocketException inner && inner.SocketErrorCode == SocketError.TimedOut;
			}
		}

		private sealed class ProtocolException : IOException
		{
			internal ProtocolException(string message) : base(message)
			{
			}
		}
	}
}
