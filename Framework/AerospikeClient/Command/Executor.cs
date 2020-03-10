/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	public sealed class Executor
	{
		private readonly List<ExecutorThread> threads;
		private volatile Exception exception;
		private int maxConcurrentThreads;
		private int completedCount;
		private volatile int done;
		private bool completed;

		public Executor(int capacity)
		{
			threads = new List<ExecutorThread>(capacity);
		}

		public void AddCommand(MultiCommand command)
		{
			threads.Add(new ExecutorThread(this, command));
		}

		public void Execute(int maxConcurrent)
		{
			// Initialize maximum number of threads to run in parallel.
			this.maxConcurrentThreads = (maxConcurrent == 0 || maxConcurrent >= threads.Count) ? threads.Count : maxConcurrent;
			
			// Start threads.
			for (int i = 0; i < maxConcurrentThreads; i++)
			{
				ThreadPool.QueueUserWorkItem(threads[i].Run);
			}
			WaitTillComplete();

			// Throw an exception if an error occurred.
			if (exception != null)
			{
				// Wrap exception because throwing will reset the exception's stack trace.
				// Wrapped exceptions preserve the stack trace in the inner exception.
				throw new AerospikeException("Command Failed: " + exception.Message, exception);
			}
		}

		public void ThreadCompleted()
		{
			int finished = Interlocked.Increment(ref completedCount);

			if (finished < threads.Count)
			{
				int nextThread = finished + maxConcurrentThreads - 1;

				// Determine if a new thread needs to be started.
				if (nextThread < threads.Count && done == 0)
				{
					// Start new thread.
					ThreadPool.QueueUserWorkItem(threads[nextThread].Run);
				}
			}
			else
			{
				// Ensure executor succeeds or fails exactly once.
				if (Interlocked.Exchange(ref done, 1) == 0)
				{
					NotifyCompleted();
				}
			}
		}

		public void StopThreads(Exception cause)
		{
			// Ensure executor succeeds or fails exactly once.
			if (Interlocked.Exchange(ref done, 1) == 0)
			{
				exception = cause;

				// Send stop signal to threads.
				foreach (ExecutorThread thread in threads)
				{
					thread.Stop();
				}
				NotifyCompleted();
			}
		}

		public bool IsDone()
		{
			return done != 0;
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
	}

	public sealed class ExecutorThread
	{
		private readonly Executor parent;
		private readonly MultiCommand command;

		public ExecutorThread(Executor parent, MultiCommand command)
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
					command.Execute();
				}
				parent.ThreadCompleted();
			}
			catch (Exception e)
			{
				// Terminate other scan threads.
				parent.StopThreads(e);
			}
		}

		/// <summary>
		/// Send stop signal to each thread.
		/// </summary>
		public void Stop()
		{
			command.Stop();
		}
	}
}
