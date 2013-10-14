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
	/// How to handle writes when the record already exists.
	/// </summary>
	public enum RecordExistsAction
	{
		/// <summary>
		/// Update record. 
		/// </summary>
		UPDATE,

		/// <summary>
		/// Update/delete record if expected generation is equal to server generation. Otherwise, fail. 
		/// </summary>
		EXPECT_GEN_EQUAL,

		/// <summary>
		/// Update/delete record if expected generation greater than the server generation. Otherwise, fail.
		/// This is useful for restore after backup. 
		/// </summary>
		EXPECT_GEN_GT,

		/// <summary>
		/// Fail if record exists. 
		/// </summary>
		FAIL
	}
}