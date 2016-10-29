/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System.Security.Authentication;

namespace Aerospike.Client
{
	/// <summary>
	/// TLS connection policy.
	/// Secure connections are only supported for AerospikeClient synchronous commands.
	/// <para>
	/// Secure connections are not supported for asynchronous commands because AsyncClient 
	/// uses the best performing SocketAsyncEventArgs.  Unfortunately, SocketAsyncEventArgs is
	/// not supported by the provided SslStream.
	/// </para>
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
		/// Default: SslProtocols.Default (SSL 3.0 or TLS 1.0) 
		/// </summary>
		public SslProtocols protocols = SslProtocols.Default;

		/// <summary>
		/// Reject certificates whose serial numbers match a serial number in this array.
		/// Default: null (Do not exclude by certificate serial number)
		/// </summary>
		public byte[][] revokeCertificates;
	
		/// <summary>
		/// Encrypt data on TLS socket only.  Do not authenticate server certificate.
		/// Default: false
		/// </summary>
		public bool encryptOnly;
	}
}
