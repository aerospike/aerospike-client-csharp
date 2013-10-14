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
	/// How to handle cases when the asynchronous maximum number of concurrent database commands have been exceeded.
	/// </summary>
	public enum MaxCommandAction
	{
		/// <summary>
		/// Accept and process command.  This implies the user is responsible for throttling asynchronous load. 
		/// </summary>
		ACCEPT,

		/// <summary>
		/// Reject database command.
		/// </summary>
		REJECT,

		/// <summary>
		/// Block until a previous command completes. 
		/// </summary>
		BLOCK,
	}
}