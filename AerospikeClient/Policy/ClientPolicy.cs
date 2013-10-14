/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
	}
}