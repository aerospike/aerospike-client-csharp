/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// TLS connection wrapper.
	/// </summary>
	public sealed class TlsConnection : Connection
	{
		private readonly SslStream sslStream;
		private readonly TlsPolicy policy;
		private readonly string tlsName;

		/// <summary>
		/// Create TLS socket and update node statistics.
		/// </summary>
		public TlsConnection(TlsPolicy policy, string tlsName, IPEndPoint address, int timeoutMillis, int maxSocketIdleMillis, Pool<Connection> pool, Node node)
			: this(policy, tlsName, address, timeoutMillis, maxSocketIdleMillis, pool)
		{
			Interlocked.Increment(ref node.connsOpened);
		}

		/// <summary>
		/// Create TLS socket.
		/// </summary>
		public TlsConnection(TlsPolicy policy, string tlsName, IPEndPoint address, int timeoutMillis, int maxSocketIdleMillis, Pool<Connection> pool)
			: base(address, timeoutMillis, maxSocketIdleMillis, pool)
		{
			this.policy = policy;

			if (tlsName == null)
			{
				tlsName = "";
			}
			this.tlsName = tlsName;

			try
			{
				RemoteCertificateValidationCallback remoteCallback = new RemoteCertificateValidationCallback(ValidateServerCertificate);
				sslStream = new SslStream(new NetworkStream(socket, true), false, remoteCallback);
#if NETFRAMEWORK
				sslStream.AuthenticateAsClient(tlsName, policy.clientCertificates, policy.protocols, false);
#else
				sslStream.AuthenticateAsClientAsync(tlsName, policy.clientCertificates, policy.protocols, false);
#endif
			}
            catch (Exception)
			{
				base.Close();
				throw;
			}
		}

		private bool ValidateServerCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors sslPolicyErrors)
		{
			// Exclude certificate serial numbers.
			if (policy.revokeCertificates != null)
			{
				byte[] serialNumber = cert.GetSerialNumber();

				foreach (byte[] sn in policy.revokeCertificates)
				{
					if (Util.ByteArrayEquals(serialNumber, sn))
					{
						if (Log.DebugEnabled())
						{
							Log.Debug("Invalid certificate serial number: " + ByteUtil.BytesToHexString(serialNumber));
						}
						return false;
					}
				}
			}

			if (sslPolicyErrors == SslPolicyErrors.None)
			{
				return true;
			}
			
			// Search subject alternative names.
			var cert2 = (X509Certificate2)cert;
			foreach (X509Extension ext in cert2.Extensions)
			{
				if (ext.Oid.Value.Equals("2.5.29.17")) // Subject Alternative Name
				{
					const string filter = "DNS Name=";
					string sans = ext.Format(false);
					string san;
					int begin = 0;
					int end;

					while ((begin = sans.IndexOf(filter, begin)) >= 0)
					{
						begin += filter.Length;
						end = sans.IndexOf(',', begin);

						if (end >= 0)
						{
							san = sans.Substring(begin, end - begin);
						}
						else
						{
							san = sans.Substring(begin);
						}

						if (san.Equals(tlsName))
						{
							return true;
						}

						if (end < 0)
						{
							break;
						}

						begin = end + 1;
					}
				}
			}

			if (Log.DebugEnabled())
			{
				Log.Debug("TLS connection error: " + sslPolicyErrors);
			}
			return false;
		}

		public override void Write(byte[] buffer, int length)
		{
			sslStream.Write(buffer, 0, length);
			sslStream.Flush();
		}

		public override void ReadFully(byte[] buffer, int length)
		{
			// The SSL stream may have already read the socket data into the stream,
			// so do not poll when SSL stream is readable.
			if (!sslStream.CanRead && socket.ReceiveTimeout > 0)
			{
				// Check if data is available for reading.
				// Poll is used because the timeout value is respected under 500ms.
				// The read method does not timeout until after 500ms.
				if (! socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
				{
					throw new SocketException((int)SocketError.TimedOut);
				}
			}

			int pos = 0;

			while (pos < length)
			{
				int count = sslStream.Read(buffer, pos, length - pos);

				if (count <= 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				pos += count;
			}
		}

		public override int Read(byte[] buffer, int offset, int length)
		{
			// The SSL stream may have already read the socket data into the stream,
			// so do not poll when SSL stream is readable.
			if (!sslStream.CanRead && socket.ReceiveTimeout > 0)
			{
				// Check if data is available for reading.
				// Poll is used because the timeout value is respected under 500ms.
				// The Receive method does not timeout until after 500ms.
				if (!socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
				{
					throw new SocketException((int)SocketError.TimedOut);
				}
			}
			return sslStream.Read(buffer, offset, length);
		}

		public override Stream GetStream()
		{
			return sslStream;
		}
	}
}
