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
	/// Asynchronous result notifications for batch exists commands.
	/// The result is sent in a single array.
	/// </summary>
	public interface ExistsArrayListener
	{
		/// <summary>
		/// This method is called when an asynchronous batch exists command completes successfully.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="keys">unique record identifiers</param>
		/// <param name="exists">whether keys exists on server</param>
		void OnSuccess(Key[] keys, bool[] exists);

		/// <summary>
		/// This method is called when an asynchronous exists command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}