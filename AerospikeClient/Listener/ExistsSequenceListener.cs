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
	/// The results are sent one record at a time.
	/// </summary>
	public interface ExistsSequenceListener
	{
		/// <summary>
		/// This method is called when an asynchronous batch exists result is received from the server.
		/// The receive sequence is not ordered.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="exists">whether key exists on server</param>
		void OnExists(Key key, bool exists);

		/// <summary>
		/// This method is called when the asynchronous batch exists command completes.
		/// </summary>
		void OnSuccess();

		/// <summary>
		/// This method is called when an asynchronous batch exists command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}