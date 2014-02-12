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
				ScanCommand command = new ScanCommand(node, policy, ns, setName, callback, binNames);
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
			private readonly ScanCommand command;
			private readonly Thread thread;

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
					command.Execute();
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
