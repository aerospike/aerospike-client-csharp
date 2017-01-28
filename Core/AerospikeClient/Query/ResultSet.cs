/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
	/// This class manages result retrieval from queries.
	/// Multiple threads will retrieve results from the server nodes and put these results on the queue.
	/// The single user thread consumes these results from the queue.
	/// </summary>
	public sealed class ResultSet : IDisposable
	{
		public static readonly object END = new object();

		private readonly QueryAggregateExecutor executor;
		private readonly BlockingCollection<object> queue;
		private readonly CancellationToken cancelToken;
		private object row;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize result set with underlying producer/consumer queue.
		/// </summary>
		public ResultSet(QueryAggregateExecutor executor, int capacity, CancellationToken cancelToken)
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
				executor.CheckForException();
				return false;
			}
			return true;
		}

		/// <summary>
		/// Close query.
		/// </summary>
		public void Dispose()
		{
			Close();
		}

		/// <summary>
		/// Close query.
		/// </summary>
		public void Close()
		{
			valid = false;

			// Check if more results are available.
			if (row != END)
			{
				if (queue.TryTake(out row) && row != END)
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

			// Send end command to transaction thread.
			// It's critical that the end token add succeeds.
			while (!queue.TryAdd(END))
			{
				// Queue must be full. Remove one item to make room.
				object tmp;
				if (!queue.TryTake(out tmp))
				{
					// Can't add or take.  Nothing can be done here.
					if (Log.DebugEnabled())
					{
						Log.Debug("ResultSet " + executor.statement.taskId + " both add and take failed on abort");
					}
					break;
				}
			}
		}
	}
}
