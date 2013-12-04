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
