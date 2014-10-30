/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;
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
		private readonly CancellationToken cancelToken;
		private KeyRecord record;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize record set with underlying producer/consumer queue.
		/// </summary>
		public RecordSet(QueryExecutor executor, int capacity, CancellationToken cancelToken)
		{
			this.executor = executor;
			this.queue = new BlockingCollection<KeyRecord>(capacity);
			this.cancelToken = cancelToken;
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
			if (!valid)
			{
				executor.CheckForException();
				return false;
			}

			try
			{
				record = queue.Take(cancelToken);
			}
			catch (OperationCanceledException)
			{
				valid = false;
				return false;
			}

			if (record == END)
			{
				valid = false;
				executor.CheckForException();
				return false;
			}

			return true;
		}

		/// <summary>
		/// Cancel query.
		/// </summary>
		public void Close()
		{
			valid = false;

			// Check if more records are available.
			if (record != END)
			{
				if (queue.TryTake(out record) && record != END)
				{
					// Some query threads may still be running. Stop these threads.
					executor.StopThreads(new AerospikeException.QueryTerminated());
				}
			}
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

		/// <summary>
		/// Get CancellationToken associated with this query.
		/// </summary>
		public CancellationToken CancelToken
		{
			get
			{
				return cancelToken;
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
			if (!valid)
			{
				return false;
			}

			try
			{
				// This add will block if queue capacity is reached.
				queue.Add(record, cancelToken);
				return true;
			}
			catch (OperationCanceledException)
			{
				// Valid may have changed.  Check again.
				if (valid)
				{
					Abort();
				}
				return false;
			}
		}

		/// <summary>
		/// Abort retrieval with end token.
		/// </summary>
		private void Abort()
		{
			valid = false;

			// Send end command to transaction thread.
			// It's critical that the end token add succeeds.
			while (!queue.TryAdd(END))
			{
				// Queue must be full. Remove one item to make room.
				KeyRecord tmp;
				if (!queue.TryTake(out tmp))
				{
					// Can't add or take.  Nothing can be done here.
					break;
				}
			}
		}
	}
}
