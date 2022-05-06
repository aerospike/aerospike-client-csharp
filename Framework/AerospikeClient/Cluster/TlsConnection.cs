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
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

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
		/// Create TLS socket.
		/// </summary>
		public TlsConnection(TlsPolicy policy, string tlsName, IPEndPoint address, int timeoutMillis, Pool<Connection> pool)
			: base(address, timeoutMillis, pool)
		{
			this.policy = policy;
			this.tlsName = tlsName;

			try
			{
				sslStream = new SslStream(new NetworkStream(socket, true), false, ValidateServerCertificate);
				sslStream.AuthenticateAsClient(tlsName, policy.clientCertificates, policy.protocols, false);
			}
            catch (Exception)
			{
				base.Close();
				throw;
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
			return ValidateCertificate(policy, tlsName, cert, sslPolicyErrors);
		}

		internal static bool ValidateCertificate
		(
			TlsPolicy policy,
			string tlsName,
			X509Certificate cert,
			SslPolicyErrors sslPolicyErrors
		)
		{
			// RemoteCertificateChainErrors will be set if the certificate is self-signed and not
			// placed in the Windows or Linux truststore. Enable the following line to temporarily
			// allow RemoteCertificateChainErrors for testing purposes.
			//
			// if (sslPolicyErrors != SslPolicyErrors.None && sslPolicyErrors != SslPolicyErrors.RemoteCertificateChainErrors)
			if (sslPolicyErrors != SslPolicyErrors.None)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Invalid certificate policy error: " + sslPolicyErrors);
				}
				return false;
			}

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

			// Search subject certificate name.
			if (FindTlsName(cert.Subject, "CN=", tlsName))
			{
				return true;
			}

			// Search subject alternative names.
			var cert2 = (X509Certificate2)cert;
			foreach (X509Extension ext in cert2.Extensions)
			{
				if (ext.Oid.Value.Equals("2.5.29.17")) // Subject Alternative Name
				{
					string str = ext.Format(false);

					// Tag is "DNS Name=" on Windows.
					if (FindTlsName(str, "DNS Name=", tlsName))
					{
						return true;
					}

					// Tag is "DNS:" on Linux.
					if (FindTlsName(str, "DNS:", tlsName))
					{
						return true;
					}
				}
			}

			if (Log.DebugEnabled())
			{
				Log.Debug("Invalid certificate, tlsName not found: " + tlsName);
			}
			return false;
		}

		private static bool FindTlsName(string str, string filter, string tlsName)
		{
			string token;
			int begin = 0;
			int end;

			while ((begin = str.IndexOf(filter, begin)) >= 0)
			{
				begin += filter.Length;
				end = str.IndexOf(',', begin);

				if (end >= 0)
				{
					token = str.Substring(begin, end - begin);
				}
				else
				{
					token = str.Substring(begin);
				}

				if (token.Equals(tlsName))
				{
					return true;
				}

				if (end < 0)
				{
					break;
				}
				begin = end + 1;
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

		public override Stream GetStream()
		{
			return sslStream;
		}
	}
}
