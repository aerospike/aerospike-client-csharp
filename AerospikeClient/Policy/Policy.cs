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
	/// Container object for transaction policy attributes used in all database
	/// operation calls.
	/// </summary>
	public class Policy
	{
		/// <summary>
		/// Priority of request relative to other transactions.
		/// Currently, only used for scans.
		/// </summary>
		public Priority priority = Priority.DEFAULT;

		/// <summary>
		/// Transaction timeout in milliseconds.
		/// This timeout is used to set the socket timeout and is also sent to the 
		/// server along with the transaction in the wire protocol.
		/// Default to no timeout (0).
		/// </summary>
		public int timeout;

		/// <summary>
		/// Maximum number of retries before aborting the current transaction.
		/// A retry is attempted when there is a network error other than timeout.  
		/// If maxRetries is exceeded, the abort will occur even if the timeout 
		/// has not yet been exceeded. The default number of retries is 2.
		/// </summary>
		public int maxRetries = 2;

		/// <summary>
		/// Milliseconds to sleep between retries if a transaction fails and the 
		/// timeout was not exceeded. The default sleep between retries is 500 ms.
		/// </summary>
		public int sleepBetweenRetries = 500;
	}
}