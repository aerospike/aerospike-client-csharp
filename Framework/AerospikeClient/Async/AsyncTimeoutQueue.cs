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
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace Aerospike.Client
{
	public sealed class AsyncTimeoutQueue
	{
		private static readonly ObjectPool<WeakReference<ITimeout>> WeakRefPool = new DefaultObjectPool<WeakReference<ITimeout>>(
			policy: new PooledWeakReferencePolicy<ITimeout>(),
			maximumRetained: 10_000);
		public static readonly AsyncTimeoutQueue Instance = new AsyncTimeoutQueue();
		private const int MIN_INTERVAL = 5;  // ms

		private readonly ConcurrentQueue<WeakReference<ITimeout>> queue = new ConcurrentQueue<WeakReference<ITimeout>>();
		private readonly LinkedList<WeakReference<ITimeout>> list = new LinkedList<WeakReference<ITimeout>>();
		private readonly Thread thread;
		private CancellationTokenSource cancel;
		private CancellationToken cancelToken;
		private volatile int sleepInterval = int.MaxValue;
		private volatile bool valid;

		public AsyncTimeoutQueue()
		{
			// Use low level Thread because system Timer class can queue up multiple simultaneous calls
			// if the callback processing time is greater than the callback interval.  This
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
			// Because commands can complete much earlier than their timeout, keeping a reference to the command until
			// their supposed timeout would make the object live a very long life promoting it to gen 2 most of the
			// time. To cope with that, only a weak reference is kept on the command so it can be garbage collected
			// after it's completed. Also, a pool of WeakReference is kept to avoid more gen 2 objects.
			var commandRef = WeakRefPool.Get();
			commandRef.SetTarget(command);
			
			queue.Enqueue(commandRef);

			if (timeout < sleepInterval)
			{
				// Enforce minimum sleep interval.
				sleepInterval = Math.Max(timeout, MIN_INTERVAL);

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
					int t = (sleepInterval == int.MaxValue) ? Timeout.Infinite : sleepInterval + 1;

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
					WeakRefPool.Return(commandRef);
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
					list.AddLast(commandRef);
				}

				WeakRefPool.Return(commandRef);
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

	public interface ITimeout
	{
		bool CheckTimeout();
	}
	
	/// <summary>
	/// A custom <see cref="PooledObjectPolicy{T}"/> is used to specify how to create a <see cref="WeakReference{T}"/>
	/// and how to reset it.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	internal class PooledWeakReferencePolicy<T> : PooledObjectPolicy<WeakReference<T>> where T : class
	{
		public override WeakReference<T> Create()
		{
			return new WeakReference<T>(null);
		}

		public override bool Return(WeakReference<T> obj)
		{
			obj.SetTarget(null);
			return true;
		}
	}
}
