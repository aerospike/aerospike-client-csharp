/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	public class ConcurrentHashMap<TKey, TValue>
	{
		private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
		private readonly Dictionary<TKey, TValue> _dictionary;

		public ConcurrentHashMap()
		{
			_dictionary = new Dictionary<TKey, TValue>();
		}

		public ConcurrentHashMap(int capacity) 
		{
			_dictionary = new Dictionary<TKey, TValue>(capacity);
		}

		public TValue this[TKey key]
		{
			get 
			{
				try
				{
					_lock.EnterReadLock();
					return _dictionary[key];
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
			set 
			{
				try
				{
					_lock.EnterWriteLock();
					_dictionary[key] = value;
				}
				finally
				{
					if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
				}
			}
		}

		public bool TryAdd(TKey key, TValue value)
		{
			_lock.EnterUpgradeableReadLock();
			try
			{
				if (!_dictionary.ContainsKey(key))
				{
					_lock.EnterWriteLock();
					try
					{
						_dictionary.Add(key, value);
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
				_dictionary.Clear();
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		public bool ContainsKey(TKey key)
		{
			try
			{
				_lock.EnterReadLock();
				return _dictionary.ContainsKey(key);
			}
			finally
			{
				if (_lock.IsReadLockHeld) _lock.ExitReadLock();
			}
		}

		public bool Remove(TKey key)
		{
			try
			{
				_lock.EnterWriteLock();
				return _dictionary.Remove(key);
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
					return _dictionary.Count;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		public bool PerformReadActionOnEachElement(Func<int, bool> initilaize, Action<TKey, TValue, int> action)
		{
			_lock.EnterReadLock();
			try
			{
				if (initilaize is null || initilaize(_dictionary.Count()))
				{
					int cnt = 0;
					foreach (var element in _dictionary)
					{
						action(element.Key, element.Value, cnt++);
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

		public bool FindElementAndPerformWriteFunc(TKey key, Func<TKey, TValue, TValue> func)
		{
			try
			{
				_lock.EnterUpgradeableReadLock();
				if (_dictionary.TryGetValue(key, out TValue value))
				{
					_lock.EnterWriteLock();
					try
					{
						_dictionary[key] = func(key, value);
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
				if (_lock.IsWriteLockHeld) _lock.ExitUpgradeableReadLock();
			}
			return false;
		}

		public bool SetValueIfNotNull(TKey key, TValue value)
		{
			try
			{
				_lock.EnterUpgradeableReadLock();
				if (_dictionary[key] == null)
				{
					_lock.EnterWriteLock();
					try
					{
						_dictionary[key] = value;
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
				if (_lock.IsReadLockHeld) _lock.ExitUpgradeableReadLock();
			}
			return false;
		}
	}
}
