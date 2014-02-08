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
		/// Create or update record.
		/// Merge write command bins with existing bins.
		/// </summary>
		UPDATE,

		/// <summary>
		/// Update record only. Fail if record does not exist.
		/// Merge write command bins with existing bins.
		/// </summary>
		UPDATE_ONLY,

		/// <summary>
		/// Create or update record.
		/// Delete existing bins not referenced by write command bins.
		/// Supported by Aerospike 2 server versions >= 2.7.5 and 
		/// Aerospike 3 server versions >= 3.1.6.
		/// </summary>
		REPLACE,

		/// <summary>
		/// Update record only. Fail if record does not exist.
		/// Delete existing bins not referenced by write command bins.
		/// Supported by Aerospike 2 server versions >= 2.7.5 and 
		/// Aerospike 3 server versions >= 3.1.6.
		/// </summary>
		REPLACE_ONLY,

		/// <summary>
		/// Create only.  Fail if record exists. 
		/// </summary>
		CREATE_ONLY,

		[System.Obsolete("Use GenerationPolicy.EXPECT_GEN_EQUAL in WritePolicy.generationPolicy instead.")]
		EXPECT_GEN_EQUAL,

		[System.Obsolete("Use GenerationPolicy.EXPECT_GEN_GT in WritePolicy.generationPolicy instead.")]
		EXPECT_GEN_GT,

		[System.Obsolete("Use RecordExistsAction.CREATE_ONLY instead.")]
		FAIL
	}
}