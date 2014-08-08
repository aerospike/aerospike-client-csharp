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
		private readonly List<BatchThread> threads;
		private volatile Exception exception;
		private int completedCount;
		private bool completed;

		public BatchExecutor
		(
			Cluster cluster,
			Policy policy,
			Key[] keys,
			bool[] existsArray,
			Record[] records,
			HashSet<string> binNames,
			int readAttr
		)
		{
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, keys);
			Dictionary<Key, BatchItem> keyMap = BatchItem.GenerateMap(keys);

			// Initialize threads.  There may be multiple threads for a single node because the
			// wire protocol only allows one namespace per command.  Multiple namespaces 
			// require multiple threads per node.
			threads = new List<BatchThread>(batchNodes.Count * 2);
			MultiCommand command = null;

			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					if (records != null)
					{
						command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keyMap, binNames, records, readAttr);
					}
					else
					{
						command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keyMap, existsArray);
					}
					threads.Add(new BatchThread(this, command));
				}
			}

			foreach (BatchThread thread in threads)
			{
				ThreadPool.QueueUserWorkItem(thread.Run);
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
			// Check if all threads completed.
			if (Interlocked.Increment(ref completedCount) >= threads.Count)
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
