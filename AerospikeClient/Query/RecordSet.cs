/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// This class manages record retrieval from queries.
	/// Multiple threads will retrieve records from the server nodes and put these records on the queue.
	/// The single user thread consumes these records from the queue.
	/// </summary>
	public sealed class RecordSet : IDisposable, IRecordSet
	{
		public static readonly KeyRecord END = new KeyRecord(null, null);

		private readonly IQueryExecutor executor;
		private readonly BlockingCollection<KeyRecord> queue;
		private readonly CancellationToken cancelToken;
		private readonly object queueMetricsLock = new();
		private int countedQueued;
		private int pendingDequeues;
		private int closed;
		private KeyRecord record;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize record set with underlying producer/consumer queue.
		/// </summary>
		public RecordSet(IQueryExecutor executor, int capacity, CancellationToken cancelToken)
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
				if (record != END)
				{
					RecordQueued(-1);
				}
			}
			catch (OperationCanceledException)
			{
				valid = false;
				executor?.CheckForException();
				return false;
			}

			if (record == END)
			{
				valid = false;
				if (executor != null) executor.CheckForException();
				return false;
			}
			return true;
		}

		public bool Disposed { get; private set; }
		private void Dispose(bool disposing)
		{
			if (!Disposed)
			{
				if (disposing)
				{
					this.Close();
					record = null;
				}

				Disposed = true;
			}
		}

		/// <summary>
		/// Dispose this record set and close the query. If records remain, producer threads
		/// are stopped. Dispose each record set before replacing it during pagination.
		/// </summary>
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Close the query. If records remain, producer threads are stopped. Calling this
		/// method or <see cref="Dispose()"/> is required when iteration ends early.
		/// </summary>
		public void Close()
		{
			if (Interlocked.CompareExchange(ref closed, 1, 0) != 0)
			{
				return;
			}
			valid = false;

			if (record != END && executor != null &&
				executor.StopThreads(new AerospikeException.QueryTerminated()))
			{
				RecordAbandoned();
			}

			// Drain every discarded item, including queues whose producers already
			// completed before Close().
			while (queue.TryTake(out KeyRecord queued))
			{
				if (queued != END)
				{
					RecordQueued(-1);
				}
			}
			record = END;
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
				if (record != END)
				{
					RecordQueued(1);
				}
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
		internal void Abort()
		{
			valid = false;

			// Discard queued records before sending the end token.
			while (queue.TryTake(out KeyRecord tmp))
			{
				if (tmp != END)
				{
					RecordQueued(-1);
				}
			}

			// Send end command to command thread.
			while (!queue.TryAdd(END))
			{
				if (queue.TryTake(out KeyRecord next) && next != END)
				{
					RecordQueued(-1);
				}
			}
		}

		private void RecordQueued(int delta)
		{
			int recordedDelta = 0;

			lock (queueMetricsLock)
			{
				if (delta > 0)
				{
					if (pendingDequeues > 0)
					{
						pendingDequeues--;
					}
					else
					{
						countedQueued++;
						recordedDelta = 1;
					}
				}
				else if (countedQueued > 0)
				{
					countedQueued--;
					recordedDelta = -1;
				}
				else
				{
					pendingDequeues++;
				}
			}

			if (recordedDelta != 0 && executor is IQueryMetrics metrics)
			{
				metrics.RecordQueued(recordedDelta);
			}
		}

		private void RecordAbandoned()
		{
			if (executor is IQueryMetrics metrics)
			{
				metrics.RecordAbandoned();
			}
		}

	}
}
