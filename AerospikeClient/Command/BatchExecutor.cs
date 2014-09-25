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
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class BatchExecutor
	{
		public static void Execute(Cluster cluster, BatchPolicy policy, Key[] keys, bool[] existsArray, Record[] records, HashSet<string> binNames, int readAttr)
		{
			if (keys.Length == 0)
			{
				return;
			}

			if (policy.allowProleReads)
			{
				// Send all requests to a single node chosen in round-robin fashion in this transaction thread.
				Node node = cluster.GetRandomNode();
				BatchCommandNodeExists command = new BatchCommandNodeExists(node, policy, keys, existsArray);
				command.Execute();
				return;
			}

			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, keys);

			if (policy.maxConcurrentThreads == 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (BatchNode batchNode in batchNodes)
				{
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						if (records != null)
						{
							BatchCommandGet command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
							command.Execute();
						}
						else
						{
							BatchCommandExists command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
							command.Execute();
						}
					}
				}
			}
			else
			{
				// Run batch requests in parallel in separate threads.
				BatchExecutor executor = new BatchExecutor(cluster, batchNodes.Count * 2);

				// Initialize threads.  There may be multiple threads for a single node because the
				// wire protocol only allows one namespace per command.  Multiple namespaces 
				// require multiple threads per node.
				foreach (BatchNode batchNode in batchNodes)
				{
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						if (records != null)
						{
							executor.Add(new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr));
						}
						else
						{
							executor.Add(new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray));
						}
					}
				}
				executor.Execute(policy);
			}
		}

		private readonly List<BatchThread> threads;
		private volatile Exception exception;
		private int completedCount;
		private int maxConcurrentThreads;
		private bool completed;

		public BatchExecutor(Cluster cluster, int capacity)
		{
			this.threads = new List<BatchThread>(capacity);
		}

		public void Add(MultiCommand command)
		{
			threads.Add(new BatchThread(this, command));
		}

		public void Execute(BatchPolicy policy)
		{
			this.maxConcurrentThreads = (policy.maxConcurrentThreads == 0 || policy.maxConcurrentThreads >= threads.Count) ? threads.Count : policy.maxConcurrentThreads;

			// Start threads.
			for (int i = 0; i < maxConcurrentThreads; i++)
			{
				ThreadPool.QueueUserWorkItem(threads[i].Run);
			}
			WaitTillComplete();

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
		
		private void ThreadCompleted()
		{
			int finished = Interlocked.Increment(ref completedCount);

			// Check if all threads completed.
			if (finished < threads.Count)
			{
				int nextThread = finished + maxConcurrentThreads - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < threads.Count)
				{
					// Start new thread.
					ThreadPool.QueueUserWorkItem(threads[nextThread].Run);
				}
			}
			else
			{
				NotifyCompleted();
			}
		}
		
		private void StopThreads(Exception cause)
		{
			lock (this)
			{
				if (exception != null)
				{
					return;
				}
				exception = cause;
			}

			foreach (BatchThread thread in threads)
			{
				try
				{
					thread.Stop();
				}
				catch (Exception)
				{
				}
			}
			NotifyCompleted();
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}

		private sealed class BatchThread
		{
			private readonly BatchExecutor parent;
			private readonly MultiCommand command;
			private Thread thread;

			public BatchThread(BatchExecutor parent, MultiCommand command)
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
					// Terminate other threads.
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
	}
}
