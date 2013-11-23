/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Threading;

namespace Aerospike.Client
{
	public abstract class QueryExecutor
	{
		protected internal readonly QueryPolicy policy;
		protected internal readonly Statement statement;
		private QueryThread[] threads;
		private volatile int nextThread;
		protected volatile Exception exception;

		public QueryExecutor(QueryPolicy policy, Statement statement)
		{
			this.policy = policy;
			this.policy.maxRetries = 0; // Retry policy must be one-shot for queries.
			this.statement = statement;
		}

		protected internal void StartThreads(Node[] nodes)
		{
			// Initialize threads.
			threads = new QueryThread[nodes.Length];

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
				threads[i].Start();
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
				threads[index].Start();
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

			if (threads != null)
			{
				foreach (QueryThread thread in threads)
				{
					try
					{
						thread.StopThread();
						thread.Interrupt();
					}
					catch (Exception)
					{
					}
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
			private readonly Thread thread;

			// It's ok to construct QueryCommand in another thread,
			// because QueryCommand no longer uses thread local data.
			internal readonly QueryCommand command;
			internal bool complete;

			public QueryThread(QueryExecutor parent, QueryCommand command)
			{
				this.parent = parent;
				this.command = command;
				this.thread = new Thread(new ThreadStart(this.Run));
			}

			public void Start()
			{
				thread.Start();
			}

			public void Run()
			{
				try
				{
					command.Execute();
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

			public void Join()
			{
				thread.Join();
			}

			public void Interrupt()
			{
				thread.Interrupt();
			}

			public void StopThread()
			{
				command.Stop();
			}
		}

		protected internal abstract QueryCommand CreateCommand(Node node);
		protected internal abstract void SendCompleted();
	}
}