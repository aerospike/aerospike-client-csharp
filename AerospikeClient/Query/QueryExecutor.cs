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
using System;
using System.Threading;

namespace Aerospike.Client
{
	public abstract class QueryExecutor
	{
		protected internal readonly QueryPolicy policy;
		protected internal readonly Statement statement;
		private readonly Node[] nodes;
		private readonly QueryThread[] threads;
		protected volatile Exception exception;
		private int nextThread;

		public QueryExecutor(Cluster cluster, QueryPolicy policy, Statement statement)
		{
			this.policy = policy;
			this.policy.maxRetries = 0; // Retry policy must be one-shot for queries.
			this.statement = statement;

			this.nodes = cluster.Nodes;

			if (this.nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.");
			}

			this.threads = new QueryThread[nodes.Length];
		}

		protected internal void StartThreads()
		{
			// Initialize threads.
			for (int i = 0; i < nodes.Length; i++)
			{
				QueryCommand command = CreateCommand(nodes[i]);
				threads[i] = new QueryThread(this, command);
			}

			// Initialize maximum number of nodes to query in parallel.
			nextThread = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.Length)? threads.Length : policy.maxConcurrentNodes;

			// Start threads. Use separate max because threadCompleted() may modify nextThread in parallel.
			int max = nextThread;

			for (int i = 0; i < max; i++)
			{
				ThreadPool.QueueUserWorkItem(threads[i].Run);
			}
		}

		private void ThreadCompleted()
		{
			int index = -1;

			// Determine if a new thread needs to be started.
			lock (threads)
			{
				if (nextThread < threads.Length)
				{
					index = nextThread++;
				}
			}

			if (index >= 0)
			{
				// Start new thread.
				ThreadPool.QueueUserWorkItem(threads[index].Run);
			}
			else
			{
				// All threads have been started. Check status.
				foreach (QueryThread thread in threads)
				{
					if (!thread.complete)
					{
						// Some threads have not finished. Do nothing.
						return;
					}
				}
				// All threads complete.  Tell RecordSet thread to return complete to user.
				SendCompleted();
			}
		}

		protected void StopThreads(Exception cause)
		{
			// Exception may be null, so can't synchronize on it.
			// Use statement instead.
			lock (statement)
			{
				if (exception != null)
				{
					return;
				}
				exception = cause;
			}

			foreach (QueryThread thread in threads)
			{
				try
				{
					thread.Stop();
				}
				catch (Exception)
				{
				}
			}
			SendCompleted();
		}

		protected internal void CheckForException()
		{
			// Throw an exception if an error occurred.
			if (exception != null)
			{
				if (exception is AerospikeException)
				{
					throw (AerospikeException)exception;
				}
				else
				{
					throw new AerospikeException(exception);
				}
			}
		}

		private sealed class QueryThread
		{
			private readonly QueryExecutor parent;
			private readonly QueryCommand command;
			private Thread thread;
			internal bool complete;

			public QueryThread(QueryExecutor parent, QueryCommand command)
			{
				this.parent = parent;
				this.command = command;
			}

			public void Run(object obj)
			{
				thread = Thread.CurrentThread;

				try
				{
					if (command.IsValid())
					{
						command.Execute();
					}
				}
				catch (Exception e)
				{
					// Terminate other query threads.
					parent.StopThreads(e);
				}
				complete = true;

				if (parent.exception == null)
				{
					parent.ThreadCompleted();
				}
			}

			public void Stop()
			{
				command.Stop();

				if (thread != null)
				{
					thread.Interrupt();
				}
			}
		}

		protected internal abstract QueryCommand CreateCommand(Node node);
		protected internal abstract void SendCompleted();
	}
}
