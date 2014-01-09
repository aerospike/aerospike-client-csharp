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
	/// Container object for policy attributes used in write operations.
	/// This object is passed into methods where database writes can occur.
	/// </summary>
	public sealed class WritePolicy : Policy
	{
		/// <summary>
		/// Qualify how to handle writes where the record already exists.
		/// </summary>
		public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

		/// <summary>
		/// Expected generation. Generation is the number of times a record has been modified
		/// (including creation) on the server. If a write operation is creating a record, 
		/// the expected generation would be 0.  
		/// </summary>
		public int generation;

		/// <summary>
		/// Record expiration.  Also known as ttl (time to live). 
        /// Seconds record will live before being removed by the server.
        /// <para>
        /// Expiration values:
        /// <list type="bullet">
        /// <item>-1: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
        /// versions >= 3.1.4.  For older servers, -1 means a very long (max integer) expiration.</item>
        /// <item>0:  Default to namespace's "default-ttl" on the server.</item>
        /// <item>> 0: Actual expiration in seconds.</item>
        /// </list>
        /// </para>
		/// </summary>
		public int expiration;
	}
}