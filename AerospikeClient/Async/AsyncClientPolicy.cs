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
	public sealed class AsyncClientPolicy : ClientPolicy
	{
		/// <summary>
		/// How to handle cases when the asynchronous maximum number of concurrent connections 
		/// have been reached.  
		/// </summary>
		public MaxCommandAction asyncMaxCommandAction = MaxCommandAction.BLOCK;

		/// <summary>
		/// Maximum number of concurrent asynchronous commands that are active at any point in time.
		/// Concurrent commands can be used to estimate concurrent connections.
		/// The number of concurrent open connections is limited by:
		/// <para>
		/// max open connections = asyncMaxCommands * &lt;number of nodes in cluster&gt;
		/// </para>
		/// The actual number of open connections consumed depends on how balanced the commands are 
		/// between nodes and if asyncMaxConnAction is ACCEPT.
		/// <para>
		/// The maximum open connections should not exceed the total socket file descriptors available
		/// on the client machine.  The socket file descriptors available can be determined by the
		/// following command:
		/// </para>
		/// <para>
		/// ulimit -n
		/// </para>
		/// </summary>
		public int asyncMaxCommands = 200;
	}
}