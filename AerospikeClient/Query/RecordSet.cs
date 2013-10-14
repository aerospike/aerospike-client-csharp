/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System.Collections.Concurrent;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// This class manages record retrieval from queries.
	/// Multiple threads will retrieve records from the server nodes and put these records on the queue.
	/// The single user thread consumes these records from the queue.
	/// </summary>
	public sealed class RecordSet
	{
		public static readonly KeyRecord END = new KeyRecord(null, null);

		private readonly QueryExecutor executor;
		private readonly BlockingCollection<KeyRecord> queue;
		private KeyRecord record;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize record set with underlying producer/consumer queue.
		/// </summary>
		public RecordSet(QueryExecutor executor, int capacity)
		{
			this.executor = executor;
			this.queue = new BlockingCollection<KeyRecord>(capacity);
		}

		//-------------------------------------------------------
		// Record traversal methods
		//-------------------------------------------------------

		/// <summary>
		/// Retrieve next record. Returns true if record exists and false if no more 
		/// records are available.
		/// This method will block until a record is retrieved or the query is cancelled.
		/// </summary>
		public bool Next()
		{
			if (valid)
			{
				try
				{
					record = queue.Take();

					if (record == END)
					{
						executor.CheckForException();
						valid = false;
					}
				}
				catch (ThreadInterruptedException)
				{
					valid = false;
				}
			}
			return valid;
		}

		/// <summary>
		/// Cancel query.
		/// </summary>
		public void Close()
		{
			valid = false;
		}

		//-------------------------------------------------------
		// Meta-data retrieval methods
		//-------------------------------------------------------

		/// <summary>
		/// Get record's unique identifier.
		/// </summary>
		public Key Key
		{
			get
			{
				return record.key;
			}
		}

		/// <summary>
		/// Get record's header and bin data.
		/// </summary>
		public Record Record
		{
			get
			{
				return record.record;
			}
		}

		//-------------------------------------------------------
		// Methods for internal use only.
		//-------------------------------------------------------

		/// <summary>
		/// Put a record on the queue.
		/// </summary>
		public bool Put(KeyRecord record)
		{
			if (valid)
			{
				try
				{
					queue.Add(record);
				}
				catch (ThreadInterruptedException)
				{
					Abort();
				}
			}
			return valid;
		}

		/// <summary>
		/// Abort retrieval with end token.
		/// </summary>
		private void Abort()
		{
			valid = false;

			// It's critical that the end put succeeds.
			// Loop through all interrupts.
			while (true)
			{
				try
				{
					queue.Add(END);
					return;
				}
				catch (ThreadInterruptedException)
				{
				}
			}
		}
	}
}