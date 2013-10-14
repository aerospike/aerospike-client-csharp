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
	/// Asynchronous result notifications for batch get and scan commands.
	/// The results are sent one record at a time.
	/// </summary>
	public interface RecordSequenceListener
	{
		/// <summary>
		/// This method is called when an asynchronous record is received from the server.
		/// The receive sequence is not ordered.
		/// <para>
		/// The user may throw a 
		/// <seealso cref="Aerospike.Client.AerospikeException.QueryTerminated"/> 
		/// exception if the command should be aborted.  If any exception is thrown, parallel command threads
		/// to other nodes will also be terminated and the exception will be propagated back through the
		/// commandFailed() call.
		/// </para>
		/// </summary>
		/// <param name="key">unique record identifier </param>
		/// <param name="record">record instance, will be null if the key is not found</param>
		/// <exception cref="AerospikeException">if error occurs or scan should be terminated.</exception>
		void OnRecord(Key key, Record record);

		/// <summary>
		/// This method is called when the asynchronous batch get or scan command completes.
		/// </summary>
		void OnSuccess();

		/// <summary>
		/// This method is called when an asynchronous batch get or scan command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}