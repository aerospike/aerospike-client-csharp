/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	public sealed class AsyncTimeoutQueue
	{
		/// <summary>
		/// Maximum number of weak references allowed in the pool.
		/// Default: 65000 
		/// </summary>
		public static int MaxWeakRefPoolCount = 65000;

		/// <summary>
		/// Maximum sleep interval in milliseconds allowed when there are active commands.
		/// Default: 1000
		/// </summary>
		private const int MaxInterval = 1000;

		/// <summary>
		/// Minimum sleep interval in milliseconds allowed.
		/// Constant: 5
		/// </summary>
		private const int MinInterval = 5;

		internal static readonly AsyncTimeoutQueue Instance = new AsyncTimeoutQueue();

		private readonly ConcurrentQueue<WeakReference<ITimeout>> weakRefPool = new ConcurrentQueue<WeakReference<ITimeout>>();
		private readonly ConcurrentQueue<WeakReference<ITimeout>> queue = new ConcurrentQueue<WeakReference<ITimeout>>();
		private readonly LinkedList<WeakReference<ITimeout>> list = new LinkedList<WeakReference<ITimeout>>();
		private readonly Thread thread;
		private CancellationTokenSource cancel;
		private CancellationToken cancelToken;
		private volatile int weakRefPoolCount;
		private volatile int sleepInterval = int.MaxValue;
		private volatile bool valid;

		public AsyncTimeoutQueue()
		{
			// Use low level Thread because system Timer class can queue up multiple simultaneous
			// calls if the callback processing time is greater than the callback interval. This
			// thread implementation only executes callback after the previous callback and another
			// interval cycle has completed.
			cancel = new CancellationTokenSource();
			cancelToken = cancel.Token;
			valid = true;
			thread = new Thread(Run)
			{
				Name = "asynctimeout",
				IsBackground = true
			};
			thread.Start();
		}

		public void Add(ITimeout command, int timeout)
		{
			WeakReference<ITimeout> commandRef = GetWeakRef(command);

			queue.Enqueue(commandRef);

			if (timeout < sleepInterval)
			{
				// Enforce minimum sleep interval.
				sleepInterval = Math.Max(timeout, MinInterval);

				lock (this)
				{
					cancel.Cancel();
				}
			}
		}

		private void Run()
		{
			while (valid)
			{
				try
				{
					int interval = sleepInterval;
					int t = (interval == int.MaxValue) ? Timeout.Infinite :
							(interval < MaxInterval) ? interval + 1 : MaxInterval;

					if (cancelToken.WaitHandle.WaitOne(t))
					{
						// Cancel signal received.  Reset token under lock.
						lock (this)
						{
							cancel.Dispose();
							cancel = new CancellationTokenSource();
							cancelToken = cancel.Token;
						}
					}

					RegisterCommands();
					CheckTimeouts();
				}
				catch (Exception e)
				{
					if (valid && Log.WarnEnabled())
					{
						Log.Warn("AsyncTimeoutQueue error: " + e.Message);
					}
				}
			}
		}

		private void RegisterCommands()
		{
			while (queue.TryDequeue(out WeakReference<ITimeout> commandRef))
			{
				// Don't add if the command was garbage collected.
				if (commandRef.TryGetTarget(out _))
				{
					list.AddLast(commandRef);
				}
				else
				{
					PutWeakRef(commandRef);
				}
			}
		}

		private void CheckTimeouts()
		{
			LinkedListNode<WeakReference<ITimeout>> node = list.First;

			if (node == null)
			{
				// Queue is empty.  Sleep until a new item is received.
				sleepInterval = int.MaxValue;
				return;
			}

			LinkedListNode<WeakReference<ITimeout>> last = list.Last;

			while (node != null)
			{
				list.RemoveFirst();

				WeakReference<ITimeout> commandRef = node.Value;

				if (commandRef.TryGetTarget(out ITimeout command) && command.CheckTimeout())
				{
					// Command is still running and has not timed out.
					// Add command reference at the end of the active list.
					list.AddLast(commandRef);
				}
				else
				{
					// Command is complete, timed out and/or garbage collected.
					// Return weak reference to pool. 
					PutWeakRef(commandRef);
				}

				if (node == last)
				{
					break;
				}
				node = list.First;
			}
		}

		private WeakReference<ITimeout> GetWeakRef(ITimeout command)
		{
			// Because commands can complete much earlier than their timeout, keeping a reference
			// to the command until their supposed timeout would make the object live a very long
			// life promoting it to gen 2 most of the time. To cope with that, only a weak reference
			// is kept on the command so it can be garbage collected after it's completed. Also, a 
			// pool of WeakReference is kept to avoid more gen 2 objects.
			if (!weakRefPool.TryDequeue(out WeakReference<ITimeout> commandRef))
			{
				commandRef = new WeakReference<ITimeout>(command);
			}
			else
			{
				Interlocked.Decrement(ref weakRefPoolCount);
				commandRef.SetTarget(command);
			}
			return commandRef;
		}

		private void PutWeakRef(WeakReference<ITimeout> commandRef)
		{
			if (weakRefPoolCount < MaxWeakRefPoolCount)
			{
				// Return weak reference to the pool.
				Interlocked.Increment(ref weakRefPoolCount);
				commandRef.SetTarget(null);
				weakRefPool.Enqueue(commandRef);
			}
		}

		public void Stop()
		{
			valid = false;
		}
	}

	public interface ITimeout
	{
		bool CheckTimeout();
	}
}
