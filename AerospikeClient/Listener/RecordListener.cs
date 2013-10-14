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
	/// Asynchronous result notifications for get or operate commands.
	/// </summary>
	public interface RecordListener
	{
		/// <summary>
		/// This method is called when an asynchronous get or operate command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="record">record instance if found, otherwise null</param>
		void OnSuccess(Key key, Record record);

		/// <summary>
		/// This method is called when an asynchronous get or operate command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}