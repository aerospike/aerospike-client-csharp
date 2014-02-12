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
using System.Collections.Concurrent;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class AsyncTimeoutQueue
	{
		public static readonly AsyncTimeoutQueue Instance = new AsyncTimeoutQueue();
		private const int MinInterval = 10;  // 10ms

		private ConcurrentQueue<AsyncCommand> queue = new ConcurrentQueue<AsyncCommand>();
		private LinkedList<AsyncCommand> list = new LinkedList<AsyncCommand>();
		private Thread thread;
		private volatile int sleepInterval = int.MaxValue;
		private volatile bool valid;

		public AsyncTimeoutQueue()
		{
			// Use low level Thread because system Timer class can queue up multiple simultaneous calls
			// if the callback processing time is greater than the callback interval.  This
  			// thread implementation only executes callback after the previous callback and another
			// interval cycle has completed.
			valid = true;
			thread = new Thread(new ThreadStart(this.Run));
			thread.Name = "asynctimeout";
			thread.IsBackground = true;
			thread.Start();
		}

		public void Add(AsyncCommand command, int timeout)
		{
			queue.Enqueue(command);

			if (timeout < sleepInterval)
			{
				sleepInterval = timeout;
				thread.Interrupt();
			}
		}

		private void Run()
		{
			while (valid)
			{
				try
				{
					int t = (sleepInterval == int.MaxValue) ? Timeout.Infinite : sleepInterval + 1;
					Thread.Sleep(t);
				}
				catch (ThreadInterruptedException)
				{
					// Sleep interrupted.  Sleep again with new timeout value.
					continue;
				}

				RegisterCommands();
				CheckTimeouts();
			}
		}

		private void RegisterCommands()
		{
			AsyncCommand command;
			while (queue.TryDequeue(out command))
			{
				list.AddLast(command);
			}
		}

		private void CheckTimeouts()
		{
			LinkedListNode<AsyncCommand> node = list.First;

			if (node == null)
			{
				// Queue is empty.  Sleep until a new item is received.
				sleepInterval = int.MaxValue;
				return;
			}

			LinkedListNode<AsyncCommand> last = list.Last;

			while (node != null)
			{
				list.RemoveFirst();

				AsyncCommand command = node.Value;

				if (command.CheckTimeout())
				{
					list.AddLast(command);
				}

				if (node == last)
				{
					break;
				}
				node = list.First;
			}
		}

		public void Stop()
		{
			valid = false;
		}
	}
}
