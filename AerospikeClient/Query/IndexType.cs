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
	/// Type of secondary index.
	/// </summary>
	public enum IndexType
	{
		/// <summary>
		/// Number index.
		/// </summary>
		NUMERIC,

		/// <summary>
		/// String index.
		/// </summary>
		STRING
	}
}