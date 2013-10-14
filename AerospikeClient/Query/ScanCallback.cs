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
	/// An object implementing this interface is passed in scan() calls, so the caller can
	/// be notified with scan results.
	/// </summary>
	public interface ScanCallback
	{
		/// <summary>
		/// This method will be called for each record returned from a scan. The user may throw a 
		/// <seealso cref="Aerospike.Client.AerospikeException.ScanTerminated"/> 
		/// exception if the scan should be aborted.  If any exception is thrown, parallel scan threads
		/// to other nodes will also be terminated and the exception will be propagated back through the
		/// initiating scan call.
		/// <para>
		/// Multiple threads will likely be calling scanCallback in parallel.  Therefore, your scanCallback
		/// implementation should be thread safe.
		/// </para>
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="record">container for bins and record meta-data</param>
		/// <exception cref="AerospikeException">if error occurs or scan should be terminated.</exception>
		void ScanCallback(Key key, Record record);
	}
}