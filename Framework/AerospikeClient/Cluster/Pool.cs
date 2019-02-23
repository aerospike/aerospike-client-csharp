/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Concurrent bounded LIFO stack with ability to pop from head or tail.
	/// <para>
	/// The standard library concurrent stack, ConcurrentStack, does not
	/// allow pop from both head and tail.
	/// </para>
	/// </summary>
	public sealed class Pool<T>
	{
		private readonly T[] items;
		private int head;
		private int tail;
		private int size;
		private volatile int total; // total items: inUse + inPool

		/// <summary>
		/// Construct stack pool.
		/// </summary>
		public Pool(int capacity)
		{
			items = new T[capacity];
		}

		/// <summary>
		/// Insert item at head of stack.
		/// </summary>
		public bool Enqueue(T item)
		{
			Monitor.Enter(this);

			try
			{
				if (size == items.Length)
				{
					return false;
				}

				items[head] = item;

				if (++head == items.Length)
				{
					head = 0;
				}
				size++;
				return true;
			}
			finally
			{
				Monitor.Exit(this);
			}
		}

		/// <summary>
		/// Insert item at tail of stack.
		/// </summary>
		public bool EnqueueLast(T item)
		{
			Monitor.Enter(this);

			try
			{
				if (size == items.Length)
				{
					return false;
				}

				if (tail == 0)
				{
					tail = items.Length - 1;
				}
				else
				{
					tail--;
				}
				items[tail] = item;
				size++;
				return true;
			}
			finally
			{
				Monitor.Exit(this);
			}
		}

		/// <summary>
		/// Pop item from head of stack.
		/// </summary>
		public bool TryDequeue(out T item)
		{
			Monitor.Enter(this);

			try
			{
				if (size == 0)
				{
					item = default(T);
					return false;
				}

				if (head == 0)
				{
					head = items.Length - 1;
				}
				else
				{
					head--;
				}
				size--;

				item = items[head];
				items[head] = default(T);
				return true;
			}
			finally
			{
				Monitor.Exit(this);
			}
		}

		/// <summary>
		/// Pop item from tail of stack.
		/// </summary>
		public bool TryDequeueLast(out T item)
		{
			Monitor.Enter(this);

			try
			{
				if (size == 0)
				{
					item = default(T);
					return false;
				}
				item = items[tail];
				items[tail] = default(T);

				if (++tail == items.Length)
				{
					tail = 0;
				}
				size--;
				return true;
			}
			finally
			{
				Monitor.Exit(this);
			}
		}

		/// <summary>
		/// Return item count.
		/// </summary>
		public int Count
		{
			get
			{
				Monitor.Enter(this);

				try
				{
					return size;
				}
				finally
				{
					Monitor.Exit(this);
				}
			}
		}

		/// <summary>
		/// Return pool capacity.
		/// </summary>
		public int Capacity
		{
			get { return items.Length; }
		}

		/// <summary>
		/// Increment total connections.
		/// </summary>
		public int IncrementTotal()
		{
			return Interlocked.Increment(ref total);
		}

		/// <summary>
		/// Decrement total connections.
		/// </summary>
		public int DecrementTotal()
		{
			return Interlocked.Decrement(ref total);
		}

		/// <summary>
		/// Return total connections.
		/// </summary>
		public int Total
		{
			get { return total; }
		}
	}
}
