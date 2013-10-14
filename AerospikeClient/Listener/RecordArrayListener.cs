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
	/// Asynchronous result notifications for batch get commands.
	/// The result is sent in a single array.
	/// </summary>
	public interface RecordArrayListener
	{
		/// <summary>
		/// This method is called when an asynchronous batch get command completes successfully.
		/// The returned record array is in positional order with the original key array order.
		/// </summary>
		/// <param name="keys">unique record identifiers</param>
		/// <param name="records">record instances, an instance will be null if the key is not found</param>
		void OnSuccess(Key[] keys, Record[] records);

		/// <summary>
		/// This method is called when an asynchronous batch get command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}