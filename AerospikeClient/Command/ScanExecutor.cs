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
	public sealed class ScanExecutor
	{
		private readonly ScanThread[] threads;
		private volatile Exception exception;
		private int nextThread;
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
			nextThread = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.Length) ? threads.Length : policy.maxConcurrentNodes;
		}

		public void ScanParallel()
		{
			// Start threads. Use separate max because threadCompleted() may modify nextThread in parallel.
			int max = nextThread;

			for (int i = 0; i < max; i++)
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
				foreach (ScanThread thread in threads)
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
			internal volatile bool complete;

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
