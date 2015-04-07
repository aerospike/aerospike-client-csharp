/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Create and manage a list within a single bin.
	/// </summary>
	public sealed class LargeList
	{
		private const string PackageName = "llist";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;

		/// <summary>
		/// Initialize large list operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		public LargeList(AerospikeClient client, WritePolicy policy, Key key, string binName)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
		}

		/// <summary>
		/// Add value to list.  Fail if value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="value">value to add</param>
		public void Add(Value value)
		{
			client.Execute(policy, key, PackageName, "add", binName, value);
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.Get(values));
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(IList values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.Get(values));
		}
		
		/// <summary>
		/// Update value in list if key exists.  Add value to list if key does not exist.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="value">value to update</param>
		public void Update(Value value)
		{
			client.Execute(policy, key, PackageName, "update", binName, value);
		}

		/// <summary>
		/// Update/Add each value in array depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "update_all", binName, Value.Get(values));
		}

		/// <summary>
		/// Update/Add each value in values list depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update(IList values)
		{
			client.Execute(policy, key, PackageName, "update_all", binName, Value.Get(values));
		}

		/// <summary>
		/// Delete value from list.
		/// </summary>
		/// <param name="value">value to delete</param>
		public void Remove(Value value)
		{
			client.Execute(policy, key, PackageName, "remove", binName, value);
		}

		/// <summary>
		/// Delete values from list.
		/// </summary>
		/// <param name="values">values to delete</param>
		public void Remove(IList values)
		{
			client.Execute(policy, key, PackageName, "remove_all", binName, Value.Get(values));
		}

		/// <summary>
		/// Delete values from list between range.  Return count of entries removed.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		public int Remove(Value begin, Value end)
		{
			object result = client.Execute(policy, key, PackageName, "remove_range", binName, begin, end);
			return (result != null) ? (int)(long)result : 0;
		}
	
		/// <summary>
		/// Select values from list.
		/// </summary>
		/// <param name="value">value to select</param>
		public IList Find(Value value)
		{
			return (IList)client.Execute(policy, key, PackageName, "find", binName, value);
		}

		/// <summary>
		/// Select values from list and apply specified Lua filter.
		/// </summary>
		/// <param name="value">value to select</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindThenFilter(Value value, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_then_filter", binName, value, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}
		
		/// <summary>
		/// Select values from the beginning of list up to a maximum count.
		/// </summary>
		/// <param name="count">maximum number of values to return</param>
		public IList FindFirst(int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_first", binName, Value.Get(count));
		}

		/// <summary>
		/// Select values from the beginning of list up to a maximum count after applying Lua filter.
		/// </summary>
		/// <param name="count">maximum number of values to return after applying Lua filter</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindFirst(int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_first", binName, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Select values from the end of list up to a maximum count.
		/// </summary>
		/// <param name="count">maximum number of values to return</param>
		public IList FindLast(int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_last", binName, Value.Get(count));
		}

		/// <summary>
		/// Select values from the end of list up to a maximum count after applying Lua filter.
		/// </summary>
		/// <param name="count">maximum number of values to return after applying Lua filter</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindLast(int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_last", binName, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Select values from the begin key up to a maximum count.
		/// </summary>
		/// <param name="begin">start value (inclusive)</param>
		/// <param name="count">maximum number of values to return</param>
		public IList FindFrom(Value begin, int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count));
		}

		/// <summary>
		/// Select values from the begin key up to a maximum count after applying Lua filter.
		/// </summary>
		/// <param name="begin">start value (inclusive)</param>
		/// <param name="count">maximum number of values to return after applying Lua filter</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindFrom(Value begin, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Select range of values from list.
		/// </summary>
		/// <param name="begin">begin value inclusive</param>
		/// <param name="end">end value inclusive</param>
		public IList Range(Value begin, Value end)
		{
			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end);
		}
		
		/// <summary>
		/// Select range of values from list.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="count">maximum number of values to return, pass in zero to obtain all values within range</param>
		public IList Range(Value begin, Value end, int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count));
		}

		/// <summary>
		/// Select range of values from the large list, then apply a Lua filter.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Range(Value begin, Value end, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end, Value.Get(filterModule), Value.Get(filterModule), Value.Get(filterArgs));
		}

		/// <summary>
		/// Select range of values from the large list up to a maximum count after applying lua filter.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="count">maximum number of values to return after applying lua filter. Pass in zero to obtain all values within range.</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Range(Value begin, Value end, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}
	
		/// <summary>
		/// Return all objects in the list.
		/// </summary>
		public IList Scan()
		{
			return (IList)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select values from list and apply specified Lua filter.
		/// </summary>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter(string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Delete bin containing the list.
		/// </summary>
		public void Destroy()
		{
			client.Execute(policy, key, PackageName, "destroy", binName);
		}

		/// <summary>
		/// Return size of list.
		/// </summary>
		public int Size()
		{
			object result = client.Execute(policy, key, PackageName, "size", binName);
			return (result != null) ? (int)(long)result : 0;
		}

		/// <summary>
		/// Return map of list configuration parameters.
		/// </summary>
		public IDictionary GetConfig()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "config", binName);
		}

		/// <summary>
		/// Set LDT page size. 
		/// </summary>
		/// <param name="pageSize">page size in bytes</param>
		public void SetPageSize(int pageSize)
		{
			client.Execute(policy, key, PackageName, "setPageSize", binName, Value.Get(pageSize));
		}
	}
}
