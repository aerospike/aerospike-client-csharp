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
	public sealed class ServerExecutor
	{
		private ServerThread[] threads;
		private Exception exception;
		private bool completed;

		public ServerExecutor
		(
			Cluster cluster,
			Policy policy,
			Statement statement,
			string packageName,
			string functionName,
			Value[] functionArgs
		)
		{
			statement.SetAggregateFunction(packageName, functionName, functionArgs, false);

			if (statement.taskId == 0)
			{
				Random r = new Random();
				statement.taskId = r.Next();
			}

			Node[] nodes = cluster.Nodes;
			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			threads = new ServerThread[nodes.Length];

			for (int i = 0; i < nodes.Length; i++)
			{
				ServerCommand command = new ServerCommand(nodes[i], policy, statement);
				threads[i] = new ServerThread(this, command);
			}

			for (int i = 0; i < nodes.Length; i++)
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
			// Check status of other threads.
			foreach (ServerThread thread in threads) 
			{
				if (! thread.complete) {
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

			foreach (ServerThread thread in threads)
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

		private sealed class ServerThread
		{
			private readonly ServerExecutor parent;
			private readonly ServerCommand command;
			private Thread thread;
			internal bool complete;

			public ServerThread(ServerExecutor parent, ServerCommand command)
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
