/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
