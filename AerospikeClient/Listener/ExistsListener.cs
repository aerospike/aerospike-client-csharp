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
	/// Asynchronous result notifications for exists commands.
	/// </summary>
	public interface ExistsListener
	{
		/// <summary>
		/// This method is called when an asynchronous exists command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="exists">whether key exists on server</param>
		void OnSuccess(Key key, bool exists);

		/// <summary>
		/// This method is called when an asynchronous exists command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}