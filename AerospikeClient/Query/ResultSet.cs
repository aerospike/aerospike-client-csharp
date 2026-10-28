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
	/// This class manages result retrieval from queries.
	/// Multiple threads will retrieve results from the server nodes and put these results on the queue.
	/// The single user thread consumes these results from the queue.
	/// </summary>
	public sealed class ResultSet : IDisposable, IResultSet
	{
		public static readonly object END = new object();

		private readonly IQueryExecutor executor;
		private readonly BlockingCollection<object> queue;
		private readonly CancellationToken cancelToken;
		private readonly object queueMetricsLock = new();
		private int countedQueued;
		private int pendingDequeues;
		private int closed;
		private object row;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize result set with underlying producer/consumer queue.
		/// </summary>
		public ResultSet(QueryAggregateExecutor executor, int capacity, CancellationToken cancelToken)
			: this((IQueryExecutor)executor, capacity, cancelToken)
		{
		}

		internal ResultSet(IQueryExecutor executor, int capacity, CancellationToken cancelToken)
		{
			this.executor = executor;
			this.queue = new BlockingCollection<object>(capacity);
			this.cancelToken = cancelToken;
		}

		//-------------------------------------------------------
		// Result traversal methods
		//-------------------------------------------------------

		/// <summary>
		/// Retrieve next result. Returns true if result exists and false if no more 
		/// results are available.
		/// This method will block until a result is retrieved or the query is cancelled.
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
				row = queue.Take(cancelToken);
				if (row != END)
				{
					RecordQueued(-1);
				}
			}
			catch (OperationCanceledException)
			{
				valid = false;
				executor.CheckForException();
				return false;
			}

			if (row == END)
			{
				valid = false;
				try
				{
					executor.CheckForException();
					((IQueryMetrics)executor).CompleteSuccess();
				}
				catch
				{
					((IQueryMetrics)executor).CompleteFailure();
					throw;
				}
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
					row = null;
				}

				Disposed = true;
			}
		}

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Close query.
		/// </summary>
		public void Close()
		{
			if (Interlocked.CompareExchange(ref closed, 1, 0) != 0)
			{
				return;
			}
			valid = false;

			if (row != END)
			{
				((IQueryMetrics)executor).RecordAbandoned();
				executor.StopThreads(new AerospikeException.QueryTerminated());
			}

			while (queue.TryTake(out object queued))
			{
				if (queued != END)
				{
					RecordQueued(-1);
				}
			}
			row = END;
		}

		//-------------------------------------------------------
		// Meta-data retrieval methods
		//-------------------------------------------------------

		/// <summary>
		/// Get result.
		/// </summary>
		public object Object
		{
			get
			{
				return row;
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
		/// Put object on the queue.
		/// </summary>
		public bool Put(object obj)
		{
			if (!valid)
			{
				return false;
			}

			try
			{
				// This add will block if queue capacity is reached.
				queue.Add(obj, cancelToken);
				if (obj != END)
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

			// Discard queued results before sending the end token.
			while (queue.TryTake(out object tmp))
			{
				if (tmp != END)
				{
					RecordQueued(-1);
				}
			}

			// Send end command to command thread.
			while (!queue.TryAdd(END))
			{
				if (queue.TryTake(out object next) && next != END)
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

			if (recordedDelta != 0)
			{
				((IQueryMetrics)executor).RecordQueued(recordedDelta);
			}
		}
	}
}
