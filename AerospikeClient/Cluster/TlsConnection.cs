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
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;

namespace Aerospike.Client
{
	/// <summary>
	/// TLS connection wrapper.
	/// </summary>
	public sealed class TlsConnection : Connection
	{
		private static readonly ConcurrentDictionary<string, byte> LegacyWarned = new ConcurrentDictionary<string, byte>();
		private static readonly Regex DottedQuad = new Regex(@"^[0-9]+(\.[0-9]+){3}$", RegexOptions.Compiled);

		private readonly SslStream sslStream;
		private readonly Cluster cluster;
		private readonly string tlsName;

		/// <summary>
		/// Create TLS socket.
		/// </summary>
		public TlsConnection(Cluster cluster, string tlsName, IPEndPoint address, int timeoutMillis, Pool<Connection> pool)
			: this(cluster, tlsName, address, timeoutMillis, null, pool)
		{
		}

		/// <summary>
		/// Create TLS socket.
		/// </summary>
		public TlsConnection(Cluster cluster, string tlsName, IPEndPoint address, int timeoutMillis, Node node, Pool<Connection> pool)
			: base(address, timeoutMillis, pool)
		{
			this.cluster = cluster;
			this.tlsName = tlsName;

			try
			{
				sslStream = new SslStream(new NetworkStream(socket, true), false, ValidateServerCertificate);
				sslStream.AuthenticateAsClient(tlsName, cluster.tlsPolicy.clientCertificates, cluster.tlsPolicy.protocols, false);
			}
			catch (Exception)
			{
				Close();

				if (node != null)
				{
					node.IncrErrorRate();
				}
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
			return ValidateCertificate(cluster, tlsName, cert, sslPolicyErrors);
		}

		internal static bool ValidateCertificate
		(
			Cluster cluster,
			string tlsName,
			X509Certificate cert,
			SslPolicyErrors sslPolicyErrors
		)
		{
			// SslStream's built-in hostname check is not RFC 9525; MatchCertificateName owns that.
			// Ignore RemoteCertificateNameMismatch so legacy CN / IP-as-dNSName fallback can warn
			// and accept, and so SAN matching is not short-circuited by SslStream.
			SslPolicyErrors errors = sslPolicyErrors & ~SslPolicyErrors.RemoteCertificateNameMismatch;

			// RemoteCertificateChainErrors will be set if the certificate is self-signed and not
			// placed in the Windows or Linux truststore. Install the CA (update-ca-certificates /
			// certutil) before connecting; do not ignore chain errors here.
			if (errors != SslPolicyErrors.None)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug(cluster.context, "Invalid certificate policy error: " + errors);
				}
				return false;
			}

			// Exclude certificate serial numbers. GetSerialNumber is little-endian; config hex is
			// typically big-endian (openssl -serial), so compare both orientations.
			if (cluster.tlsPolicy.revokeCertificates != null)
			{
				byte[] serialLe = cert.GetSerialNumber();
				byte[] serialBe = (byte[])serialLe.Clone();
				Array.Reverse(serialBe);

				foreach (byte[] sn in cluster.tlsPolicy.revokeCertificates)
				{
					if (Util.ByteArrayEquals(serialLe, sn) || Util.ByteArrayEquals(serialBe, sn))
					{
						throw new AerospikeException("Invalid certificate serial number: " +
							ByteUtil.BytesToHexString(serialBe));
					}
				}
			}

			if (MatchCertificateName(cert, tlsName, cluster.context))
			{
				return true;
			}

			throw new AerospikeException("Invalid TLS name: " + tlsName);
		}

		/// <summary>
		/// Validate that <paramref name="tlsName"/> matches the certificate per RFC 9525 (with the
		/// deprecated CN/legacy dNSName fallback). Used by L1 conformance. Throws
		/// <see cref="AerospikeException"/> on mismatch.
		/// </summary>
		internal static void ValidateServerCertificate(string tlsName, X509Certificate cert)
		{
			if (tlsName == null)
			{
				throw new AerospikeException("Invalid TLS name: null");
			}

			if (MatchCertificateName(cert, tlsName, null))
			{
				return;
			}

			throw new AerospikeException("Invalid TLS name: " + tlsName);
		}

		/// <summary>
		/// If tlsName is an IPv4 or IPv6 literal, it is an IP-ID and matches only an iPAddress SAN
		/// holding the same address. Otherwise it is a DNS-ID and matches only a dNSName SAN,
		/// ASCII case-insensitive. The only wildcard is a left-most label of exactly "*".
		/// Deprecated legacy fallback: exact (case-sensitive) subject CN or dNSName equality.
		/// </summary>
		private static bool MatchCertificateName(X509Certificate cert, string tlsName, Log.Context context)
		{
			if (tlsName == null)
			{
				return false;
			}

			X509Certificate2 cert2 = cert as X509Certificate2 ?? new X509Certificate2(cert);
			X509SubjectAlternativeNameExtension san = FindSan(cert2);

			if (san != null)
			{
				byte[] ip = ParseIpLiteral(tlsName);
				bool ipId = ip != null || IsIpId(tlsName);

				if (ipId)
				{
					if (ip != null)
					{
						foreach (IPAddress address in san.EnumerateIPAddresses())
						{
							if (Util.ByteArrayEquals(ip, address.GetAddressBytes()))
							{
								return true;
							}
						}
					}
				}
				else
				{
					foreach (string dns in san.EnumerateDnsNames())
					{
						if (MatchDnsName(dns, tlsName))
						{
							return true;
						}
					}
				}
			}

			if (MatchLegacyName(tlsName, cert2, san))
			{
				if (Log.WarnEnabled() && LegacyWarned.TryAdd(tlsName, 0))
				{
					string message = "TLS name '" + tlsName +
						"' matched the server certificate only by the legacy rule " +
						"(subject CN or IP address as dNSName). This is not RFC 9525 conformant and will be " +
						"rejected in the next major release.";

					if (context != null)
					{
						Log.Warn(context, message);
					}
					else
					{
						Log.Warn(message);
					}
				}

				return true;
			}

			return false;
		}

		private static X509SubjectAlternativeNameExtension FindSan(X509Certificate2 cert)
		{
			foreach (X509Extension ext in cert.Extensions)
			{
				if (ext.Oid != null && ext.Oid.Value.Equals("2.5.29.17"))
				{
					return new X509SubjectAlternativeNameExtension(ext.RawData, false);
				}
			}

			return null;
		}

		private static bool MatchLegacyName(string tlsName, X509Certificate2 cert, X509SubjectAlternativeNameExtension san)
		{
			string cn = cert.GetNameInfo(X509NameType.SimpleName, false);

			if (!string.IsNullOrEmpty(cn) && cn.Equals(tlsName))
			{
				return true;
			}

			if (san != null)
			{
				foreach (string dns in san.EnumerateDnsNames())
				{
					if (dns.Equals(tlsName))
					{
						return true;
					}
				}
			}

			return false;
		}

		private static bool MatchDnsName(string pattern, string name)
		{
			if (pattern.StartsWith("*.", StringComparison.Ordinal))
			{
				if (pattern.IndexOf('*', 1) >= 0)
				{
					return false;
				}

				int dot = name.IndexOf('.');
				return dot > 0 && EqualsIgnoreCaseAscii(pattern.Substring(1), name.Substring(dot));
			}

			return pattern.IndexOf('*') < 0 && EqualsIgnoreCaseAscii(pattern, name);
		}

		private static bool EqualsIgnoreCaseAscii(string a, string b)
		{
			if (a.Length != b.Length)
			{
				return false;
			}

			for (int i = 0; i < a.Length; i++)
			{
				char x = a[i];
				char y = b[i];

				if (x != y && ToLowerAscii(x) != ToLowerAscii(y))
				{
					return false;
				}
			}

			return true;
		}

		private static char ToLowerAscii(char c)
		{
			return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
		}

		private static bool IsIpId(string s)
		{
			return s.IndexOf(':') >= 0 || DottedQuad.IsMatch(s);
		}

		/// <summary>
		/// Parse IPv4 or IPv6 literal without name resolution. Return null if not a literal.
		/// </summary>
		private static byte[] ParseIpLiteral(string s)
		{
			if (s.Length > 2 && s[0] == '[' && s[s.Length - 1] == ']')
			{
				s = s.Substring(1, s.Length - 2);
			}

			// Zone/scope IDs (fe80::1%en0) are not RFC 9525 IP-IDs; .NET TryParse strips the
			// suffix and would otherwise match an iPAddress SAN for the unscoped address.
			if (s.IndexOf('%') >= 0)
			{
				return null;
			}

			if (!IPAddress.TryParse(s, out IPAddress address))
			{
				return null;
			}

			// Reject IPv4 with leading zeros (Java parity / RFC 5952 style).
			if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
			{
				string[] parts = s.Split('.');

				if (parts.Length != 4)
				{
					return null;
				}

				foreach (string part in parts)
				{
					if (part.Length > 1 && part[0] == '0')
					{
						return null;
					}
				}
			}

			return address.GetAddressBytes();
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
				if (!socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
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
				received = true;
				pos += count;
			}
		}

		public override void ReadFully(byte[] buffer, int length, byte state)
		{
			int count = 0;

			// The SSL stream may have already read the socket data into the stream,
			// so do not poll when SSL stream is readable.
			if (!sslStream.CanRead && socket.ReceiveTimeout > 0)
			{
				// Check if data is available for reading.
				// Poll is used because the timeout value is respected under 500ms.
				// The read method does not timeout until after 500ms.
				if (!socket.Poll(socket.ReceiveTimeout * 1000, SelectMode.SelectRead))
				{
					throw new ReadTimeout(buffer, 0, length, state);
				}
			}

			int pos = 0;

			while (pos < length)
			{
				try
				{
					count = sslStream.Read(buffer, pos, length - pos);
				}
				catch (IOException io)
				{
					if (io.InnerException is SocketException ae && ae.SocketErrorCode == SocketError.TimedOut)
					{
						throw new ReadTimeout(buffer, pos, length, state);
					}
					throw;
				}

				if (count <= 0)
				{
					throw new SocketException((int)SocketError.ConnectionReset);
				}
				received = true;
				pos += count;
			}
		}

		public override Stream GetStream()
		{
			return sslStream;
		}

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public override void Close()
		{
			base.Close();
			sslStream.Dispose();
		}
	}
}
