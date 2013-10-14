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
	/// Asynchronous result notifications for delete commands.
	/// </summary>
	public interface DeleteListener
	{
		/// <summary>
		/// This method is called when an asynchronous delete command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="existed">whether record existed on server before deletion</param>
		void OnSuccess(Key key, bool existed);

		/// <summary>
		/// This method is called when an asynchronous delete command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}