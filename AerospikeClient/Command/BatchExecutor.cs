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
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class BatchExecutor
	{
		private readonly List<BatchThread> threads;
		private volatile Exception exception;
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
			// Check status of other threads.
			foreach (BatchThread thread in threads)
			{
				if (!thread.complete)
				{
					// Some threads have not finished. Do nothing.
					return;
				}
			}
			// All threads complete.
			NotifyCompleted();
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
			internal bool complete;

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
	}
}
