/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	public abstract class QueryExecutor : IQueryExecutor
	{
		protected internal readonly Cluster cluster;
		protected internal readonly QueryPolicy policy;
		protected internal readonly Statement statement;
		protected internal readonly ulong taskId;
		private readonly Node[] nodes;
		private readonly QueryThread[] threads;
		protected internal readonly CancellationTokenSource cancel;
		protected volatile Exception exception;
		private readonly int maxConcurrentNodes;
		private int completedCount;
		private int done;

		public QueryExecutor(Cluster cluster, QueryPolicy policy, Statement statement, Node[] nodes)
		{
			this.cluster = cluster;
			this.policy = policy;
			this.statement = statement;
			this.taskId = statement.PrepareTaskId();
			this.nodes = nodes;
			this.threads = new QueryThread[nodes.Length];
			this.cancel = new CancellationTokenSource();

			// Initialize maximum number of nodes to query in parallel.
			this.maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.Length) ? threads.Length : policy.maxConcurrentNodes;
		}

		protected internal void InitializeThreads()
		{
			// Detect cluster migrations when performing scan.
			ulong clusterKey = policy.failOnClusterChange ? QueryValidate.ValidateBegin(nodes[0], statement.ns) : 0;	
			bool first = true;

			// Initialize threads.
			for (int i = 0; i < nodes.Length; i++)
			{
				MultiCommand command = CreateCommand(nodes[i], clusterKey, first);
				threads[i] = new QueryThread(this, command);
				first = false;
			}
		}

		protected internal void StartThreads()
		{
			// Start threads.
			for (int i = 0; i < maxConcurrentNodes; i++)
			{
				ThreadPool.UnsafeQueueUserWorkItem(threads[i].Run, null);
			}
		}

		private void ThreadCompleted()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < threads.Length)
			{
				int nextThread = finished + maxConcurrentNodes - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < threads.Length && done == 0)
				{
					// Start new thread.
					ThreadPool.UnsafeQueueUserWorkItem(threads[nextThread].Run, null);
				}
			}
			else
			{
				// All threads complete.  Tell RecordSet thread to return complete to user
				// if an exception has not already occurred.
				if (Interlocked.Exchange(ref done, 1) == 0)
				{
					SendCompleted();
				}
			}
		}

		public bool StopThreads(Exception cause)
		{
			// There is no need to stop threads if all threads have already completed.
			if (Interlocked.Exchange(ref done, 1) == 0)
			{
				exception = cause;

				foreach (QueryThread thread in threads)
				{
					thread.Stop();
				}
				cancel.Cancel();
				SendCancel();
				return true;
			}
			return false;
		}

		public void CheckForException()
		{
			// Throw an exception if an error occurred.
			if (exception != null)
			{
				// Wrap exception because throwing will reset the exception's stack trace.
				// Wrapped exceptions preserve the stack trace in the inner exception.
				throw new AerospikeException("Query Failed: " + exception.Message, exception);
			}
		}

		private sealed class QueryThread
		{
			private readonly QueryExecutor parent;
			private readonly MultiCommand command;

			public QueryThread(QueryExecutor parent, MultiCommand command)
			{
				this.parent = parent;
				this.command = command;
			}

			public void Run(object obj)
			{
				try
				{
					if (command.IsValid())
					{
						command.ExecuteAndValidate();
					}
					parent.ThreadCompleted();
				}
				catch (Exception e)
				{
					// Terminate other query threads.
					parent.StopThreads(e);
				}
			}

			public void Stop()
			{
				command.Stop();
			}
		}

		protected internal abstract MultiCommand CreateCommand(Node node, ulong clusterKey, bool first);
		protected internal abstract void SendCancel();
		protected internal abstract void SendCompleted();
	}
}
