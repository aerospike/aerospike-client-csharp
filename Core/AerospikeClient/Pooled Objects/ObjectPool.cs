/* 
 * Copyright 2012-2017 Aerospike, Inc.
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

namespace AerospikeClient.Pooled_Objects
{
    using System;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    ///  Pool of objects to re-use intanes of objects of <typeparam name="T">T</typeparam>.
    /// 
    ///  The idea of this pool is to quickly re-use instances if you try to hold intances for long it is not
    ///  efficient consider using new instead.
    /// </summary>
    /// <typeparam name="T">Type of pool contained of. </typeparam>
    internal class ObjectPool<T> : IObjectPool<T> where T : class
    {
        private readonly Func<T> _factory;
        private T _firstItem;
        // We use array because of the cache friendliness
        private readonly T[] _items;

        public ObjectPool(Func<T> factory) : this(factory, Environment.ProcessorCount * 2) { }

        public ObjectPool(Func<T> factory, int size)
        {
            Debug.Assert(size >= 1);
            _factory = factory;
            _items = new T[size - 1];
        }

        /// <summary>
        ///  Allocates item in the pool, this willeither return new instance or already allocated one.
        /// </summary>
        /// <returns> Intance of T. </returns>
        public virtual T Allocate()
        {
            // Try to get first object first.
            var inst = _firstItem;
            if (inst == null || inst != Interlocked.CompareExchange(ref _firstItem, null, inst))
            {
                // If first is null simply search for the next non-null object in the items.
                var items = _items;
                for (var i = 0; i < items.Length; i++)
                {
                    var inst2 = items[i];
                    // return first object that we find it is not null
                    if (inst2 != null && inst2 == Interlocked.CompareExchange(ref items[i], null, inst2))
                    {
                        return inst2;
                    }
                }
                // If we fail to find initialized object, create one ourselves.
                return _factory();
            }
            // return instance of the first object (this is not null for sure).
            return inst;
        }
        /// <summary>
        ///  Returns item back to the pool.
        /// </summary>
        /// <param name="obj"> The item to return. </param>
        public virtual void Free(T obj)
        {
            Debug.Assert(obj != null);
            Debug.Assert(obj != _firstItem);

            // Try to return object as first first.
            if (_firstItem == null)
            {
                _firstItem = obj;
            }
            // If we fail we return it to the next best place.
            else
            {
                var items = _items;
                for (var i = 0; i < items.Length; i++)
                {
                    if (items[i] == null)
                    {
                        items[i] = obj;
                        break;
                    }
                }
            }
        }
        /// <summary>
        ///  Forgets about intance in the pool, this should be used when 
        ///  heavy objects are allocated so we dont store them in memory, it 
        ///  will be inefficient, that is not the idea of the pool.
        /// </summary>
        /// <param name="obj"> The instance to remove from the pool. </param>
        public virtual void Forget(T obj)
        {
            if (obj == null) { return; }

            for (var i = 0; i < _items.Length; i++)
            {
                Interlocked.CompareExchange(ref _items[i], null, obj);
            }
        }
    }
}
