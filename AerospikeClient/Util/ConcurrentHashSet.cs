/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class ConcurrentHashSet<T> : IDisposable
	{
		private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.SupportsRecursion);
		private readonly HashSet<T> _hashSet;
		private bool disposedValue;

		public ConcurrentHashSet()
		{
			_hashSet = [];
		}

		public ConcurrentHashSet(int capacity)
		{
			_hashSet = new HashSet<T>(capacity);
		}

		public bool Add(T item)
		{
			_lock.EnterUpgradeableReadLock();
			try
			{
				if (!_hashSet.Contains(item))
				{
					_lock.EnterWriteLock();
					try
					{
						_hashSet.Add(item);
					}
					finally
					{
						_lock.ExitWriteLock();
					}
					return true;
				}
			}
			finally
			{
				_lock.ExitUpgradeableReadLock();
			}
			return false;
		}

		public void Clear()
		{
			try
			{
				_lock.EnterWriteLock();
				_hashSet.Clear();
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		public bool Contains(T item)
		{
			try
			{
				_lock.EnterReadLock();
				return _hashSet.Contains(item);
			}
			finally
			{
				if (_lock.IsReadLockHeld) _lock.ExitReadLock();
			}
		}

		public bool Remove(T item)
		{
			try
			{
				_lock.EnterWriteLock();
				return _hashSet.Remove(item);
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		public int Count
		{
			get
			{
				try
				{
					_lock.EnterReadLock();
					return _hashSet.Count;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		public bool PerformActionOnEachElement(Func<int, bool> initilaize, Action<T, int> action)
		{
			_lock.EnterReadLock();
			try
			{
				if (initilaize is null || initilaize(_hashSet.Count()))
				{
					int cnt = 0;
					foreach (var element in _hashSet)
					{
						action(element, cnt++);
					}
					return cnt > 0;
				}
			}
			finally
			{
				_lock.ExitReadLock();
			}
			return false;
		}

		private void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					// Dispose managed state (managed objects)
					_lock.Dispose();
				}

				disposedValue = true;
			}
		}

		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}
	}
}
