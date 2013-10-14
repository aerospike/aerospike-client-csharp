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
	/// Priority of operations on database server.
	/// </summary>
	public enum Priority
	{
		/// <summary>
		/// The server defines the priority.
		/// </summary>
		DEFAULT,

		/// <summary>
		/// Run the database operation in a background thread.
		/// </summary>
		LOW,

		/// <summary>
		/// Run the database operation at medium priority.
		/// </summary>
		MEDIUM,

		/// <summary>
		/// Run the database operation at the highest priority.
		/// </summary>
		HIGH
	}
}