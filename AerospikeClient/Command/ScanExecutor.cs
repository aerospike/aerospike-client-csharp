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
	public sealed class ScanExecutor
	{
		private readonly ScanThread[] threads;
		private volatile Exception exception;
		private readonly int maxConcurrentNodes;
		private int completedCount;
		private bool completed;

		public ScanExecutor
		(
			Cluster cluster,
			Node[] nodes,
			ScanPolicy policy,
			string ns,
			string setName,
			ScanCallback callback,
			string[] binNames
		)
		{
			// Initialize threads.		
			threads = new ScanThread[nodes.Length];

			for (int i = 0; i < nodes.Length; i++)
			{
				ScanCommand command = new ScanCommand(nodes[i], policy, ns, setName, callback, binNames);
				threads[i] = new ScanThread(this, command);
			}

			// Initialize maximum number of nodes to query in parallel.
			maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.Length) ? threads.Length : policy.maxConcurrentNodes;
		}

		public void ScanParallel()
		{
			// Start threads.
			for (int i = 0; i < maxConcurrentNodes; i++)
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

			foreach (ScanThread thread in threads)
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

		private sealed class ScanThread
		{
			private readonly ScanExecutor parent;
			private readonly ScanCommand command;
			private Thread thread;

			public ScanThread(ScanExecutor parent, ScanCommand command)
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
					// Terminate other scan threads.
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
