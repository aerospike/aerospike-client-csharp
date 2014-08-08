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
using System.Collections.Concurrent;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// This class manages record retrieval from queries.
	/// Multiple threads will retrieve records from the server nodes and put these records on the queue.
	/// The single user thread consumes these records from the queue.
	/// </summary>
	public sealed class ResultSet
	{
		public static readonly object END = new object();

		private readonly QueryAggregateExecutor executor;
		private readonly BlockingCollection<object> queue;
		private object row;
		private volatile bool valid = true;

		/// <summary>
		/// Initialize record set with underlying producer/consumer queue.
		/// </summary>
		public ResultSet(QueryAggregateExecutor executor, int capacity)
		{
			this.executor = executor;
			this.queue = new BlockingCollection<object>(capacity);
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
					row = queue.Take();

					if (row == END)
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
		/// Get record's header and bin data.
		/// </summary>
		public object Object
		{
			get
			{
				return row;
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
			if (valid)
			{
				try
				{
					queue.Add(obj);
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
