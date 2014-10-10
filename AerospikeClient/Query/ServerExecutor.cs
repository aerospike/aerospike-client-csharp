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
	public sealed class ServerExecutor
	{
		private ServerThread[] threads;
		private Exception exception;
		private int completedCount;
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
			statement.Prepare();

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
			// Check if all threads completed.
			if (Interlocked.Increment(ref completedCount) >= threads.Length)
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
