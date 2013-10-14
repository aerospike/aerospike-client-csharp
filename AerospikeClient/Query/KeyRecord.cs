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
	/// Container object for key identifier and record data.
	/// </summary>
	public sealed class KeyRecord
	{
		/// <summary>
		/// Unique identifier for record.
		/// </summary>
		public readonly Key key;

		/// <summary>
		/// Record header and bin data.
		/// </summary>
		public readonly Record record;

		/// <summary>
		/// Initialize key and record.
		/// </summary>
		public KeyRecord(Key key, Record record)
		{
			this.key = key;
			this.record = record;
		}
	}
}