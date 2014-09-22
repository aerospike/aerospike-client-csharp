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
		private readonly int maxConcurrentNodes;
		private int completedCount;

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

			// Initialize maximum number of nodes to query in parallel.
			this.maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.Length) ? threads.Length : policy.maxConcurrentNodes;
		}

		protected internal void StartThreads()
		{
			// Initialize threads.
			for (int i = 0; i < nodes.Length; i++)
			{
				QueryCommand command = CreateCommand(nodes[i]);
				threads[i] = new QueryThread(this, command);
			}

			// Start threads.
			for (int i = 0; i < maxConcurrentNodes; i++)
			{
				ThreadPool.QueueUserWorkItem(threads[i].Run);
			}
		}

		private void ThreadCompleted()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < threads.Length)
			{
				int nextThread = finished + maxConcurrentNodes - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < threads.Length)
				{
					// Start new thread.
					ThreadPool.QueueUserWorkItem(threads[nextThread].Run);
				}
			}
			else
			{
				// All threads complete.  Tell RecordSet thread to return complete to user.
				SendCompleted();
			}
		}

		protected internal void StopThreads(Exception cause)
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
