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
	public sealed class ScanExecutor
	{
		private readonly ScanPolicy policy;
		private readonly string ns;
		private readonly string setName;
		private readonly ScanCallback callback;
		private readonly string[] binNames;
		private ScanThread[] threads;
		private Exception exception;

		public ScanExecutor(ScanPolicy policy, string ns, string setName, ScanCallback callback, string[] binNames)
		{
			this.policy = policy;
			this.ns = ns;
			this.setName = setName;
			this.callback = callback;
			this.binNames = binNames;
		}

		public void ScanParallel(Node[] nodes)
		{
			threads = new ScanThread[nodes.Length];
			int count = 0;

			foreach (Node node in nodes)
			{
				ScanCommand command = new ScanCommand(node, callback);
				ScanThread thread = new ScanThread(this, command);
				threads[count++] = thread;
				thread.Start();
			}

			foreach (ScanThread thread in threads)
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

			foreach (ScanThread thread in threads)
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

		private sealed class ScanThread
		{
			private readonly ScanExecutor parent;
			private readonly Thread thread;

			// It's ok to construct ScanCommand in another thread,
			// because ScanCommand no longer uses thread local data.
			internal readonly ScanCommand command;

			public ScanThread(ScanExecutor parent, ScanCommand command)
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
					command.SetScan(parent.policy, parent.ns, parent.setName, parent.binNames);
					command.Execute(parent.policy);
				}
				catch (Exception e)
				{
					// Terminate other scan threads.
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