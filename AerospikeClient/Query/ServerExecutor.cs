/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
		}

		public void Execute(Node[] nodes)
		{
			threads = new ServerThread[nodes.Length];
			int count = 0;

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(node);
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
					command.Query(parent.policy, parent.statement);
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