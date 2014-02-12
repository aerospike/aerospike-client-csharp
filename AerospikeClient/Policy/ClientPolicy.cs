/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Container object for client policy Command.
	/// </summary>
	public class ClientPolicy
	{
		/// <summary>
		/// Initial host connection timeout in milliseconds.  The timeout when opening a connection 
		/// to the server host for the first time.
		/// </summary>
		public int timeout = 1000;

		/// <summary>
		/// Estimate of incoming threads concurrently using synchronous methods in the client instance.
		/// This field is used to size the synchronous connection pool for each server node.
		/// </summary>
		public int maxThreads = 300;

		/// <summary>
		/// Maximum socket idle in seconds.  Socket connection pools will discard sockets
		/// that have been idle longer than the maximum.
		/// </summary>
		public int maxSocketIdle = 14;

		/// <summary>
		/// Throw exception if host connection fails during addHost().
		/// </summary>
		public bool failIfNotConnected;

		/// <summary>
		/// A IP translation table is used in cases where different clients use different server 
		/// IP addresses.  This may be necessary when using clients from both inside and outside 
		/// a local area network.  Default is no translation.
		/// 
		/// The key is the IP address returned from friend info requests to other servers.  The 
		/// value is the real IP address used to connect to the server.
		/// </summary>
		public Dictionary<string, string> ipMap;
	}
}
