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
	/// Asynchronous result notifications for put, append, prepend, add, delete and touch commands.
	/// </summary>
	public interface WriteListener
	{
		/// <summary>
		/// This method is called when an asynchronous write command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		void OnSuccess(Key key);

		/// <summary>
		/// This method is called when an asynchronous write command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}