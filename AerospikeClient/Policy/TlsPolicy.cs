/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Aerospike.Client
{
	/// <summary>
	/// TLS connection policy.
	/// Secure connections are supported for AerospikeClient synchronous commands 
	/// and asynchronous commands.
	/// </summary>
	public sealed class TlsPolicy
	{
		/// <summary>
		/// Allowable TLS protocols that the client can use for secure connections.
		/// Multiple protocols can be specified.  Example:
		/// <code>
		/// TlsPolicy policy = new TlsPolicy();
		/// policy.protocols = SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12;
		/// </code>
		/// Default: SslProtocols.Tls12 
		/// </summary>
		public SslProtocols protocols = SslProtocols.Tls12;

		/// <summary>
		/// Reject server certificates whose serial numbers match a serial number in this array.
		/// <para>Default: null (Do not exclude by certificate serial number)</para>
		/// </summary>
		public byte[][] revokeCertificates;

		/// <summary>
		/// Client certificates to pass to server when server requires mutual authentication.
		/// <para>Default: null (Client authenticates server, but server does not authenticate client)</para>
		/// </summary>
		public X509CertificateCollection clientCertificates;

		/// <summary>
		/// Use TLS connections only for login authentication.  All other communication with
		/// the server will be done with non-TLS connections. 
		/// <para>Default: false (Use TLS connections for all communication with server)</para>
		/// </summary>
		public bool forLoginOnly;

		/// <summary>
		/// Default constructor.
		/// </summary>
		public TlsPolicy()
		{
		}

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public TlsPolicy(TlsPolicy other)
		{
			this.protocols = other.protocols;
			this.revokeCertificates = other.revokeCertificates;
			this.clientCertificates = other.clientCertificates;
			this.forLoginOnly = other.forLoginOnly;
		}

		/// <summary>
		/// Constructor for TLS properties.
		/// </summary>
		public TlsPolicy(string protocolString, string revokeString, string clientCertificateFile, bool forLoginOnly)
		{
			ParseSslProtocols(protocolString);
			ParseRevokeString(revokeString);
			ParseClientCertificateFile(clientCertificateFile);
			this.forLoginOnly = forLoginOnly;
		}

		private void ParseSslProtocols(string protocolString)
		{
			if (protocolString == null)
			{
				return;
			}

			protocolString = protocolString.Trim();

			if (protocolString.Length == 0)
			{
				return;
			}

			protocols = SslProtocols.None;
			string[] list = protocolString.Split(',');

			foreach (string item in list)
			{
				string s = item.Trim();

				if (s.Length > 0)
				{
					protocols |= (SslProtocols)Enum.Parse(typeof(SslProtocols), s);
				}
			}
		}

		private void ParseRevokeString(string revokeString)
		{
			if (revokeString == null)
			{
				return;
			}

			revokeString = revokeString.Trim();

			if (revokeString.Length == 0)
			{
				return;
			}

			revokeCertificates = Util.HexStringToByteArrays(revokeString);
		}

		private void ParseClientCertificateFile(string clientCertificateFile)
		{
			if (clientCertificateFile == null)
			{
				return;
			}

			clientCertificateFile = clientCertificateFile.Trim();

			if (clientCertificateFile.Length == 0)
			{
				return;
			}

			X509Certificate2 cert = new X509Certificate2(clientCertificateFile);
			clientCertificates = new X509CertificateCollection();
			clientCertificates.Add(cert);
		}

		/// <summary>
		/// Creates a deep copy of this TLS policy.
		/// </summary>
		/// <returns></returns>
		public TlsPolicy Clone()
		{
			return new TlsPolicy(this);
		}
	}
}
