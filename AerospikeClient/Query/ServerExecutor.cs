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
		private readonly Policy policy;
		private readonly Statement statement;
		private ServerThread[] threads;
		private Exception exception;

		public ServerExecutor(Policy policy, Statement statement, string packageName, string functionName, Value[] functionArgs)
		{
			this.policy = policy;
			this.statement = statement;
			this.statement.SetAggregateFunction(packageName, functionName, functionArgs, false);

			if (this.statement.taskId == 0)
			{
				Random r = new Random();
				this.statement.taskId = r.Next();
			}
		}

		public void Execute(Node[] nodes)
		{
			threads = new ServerThread[nodes.Length];
			int count = 0;

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(node, policy, statement);
				ServerThread thread = new ServerThread(this, command);
				threads[count++] = thread;
				thread.Start();
			}

			foreach (ServerThread thread in threads)
			{
				try
				{
					thread.Join();
				}
				catch (Exception)
				{
				}
			}

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
					thread.StopThread();
					thread.Interrupt();
				}
				catch (Exception)
				{
				}
			}
		}

		private sealed class ServerThread
		{
			private readonly ServerExecutor parent;
			private readonly Thread thread;
			private readonly ServerCommand command;

			public ServerThread(ServerExecutor parent, ServerCommand command)
			{
				this.parent = parent;
				this.command = command;
				this.thread = new Thread(new ThreadStart(this.Run));
			}

			public void Start()
			{
				thread.Start();
			}

			public void Run()
			{
				try
				{
					command.Execute();
				}
				catch (Exception e)
				{
					// Terminate other threads.
					parent.StopThreads(e);
				}
			}

			public void Join()
			{
				thread.Join();
			}

			public void Interrupt()
			{
				thread.Interrupt();
			}

			public void StopThread()
			{
				command.Stop();
			}
		}
	}
}
