/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
		private readonly Value userModule;

		/// <summary>
		/// Initialize large list operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default list</param>
		public LargeList(AerospikeClient client, WritePolicy policy, Key key, string binName, string userModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.userModule = Value.Get(userModule);
		}

		/// <summary>
		/// Add value to list.  Fail if value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to add</param>
		public void Add(Value value)
		{
			client.Execute(policy, key, PackageName, "add", binName, value, userModule);
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.Get(values), userModule);
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(IList values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.GetAsList(values), userModule);
		}
		
		/// <summary>
		/// Update value in list if key exists.  Add value to list if key does not exist.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to update</param>
		public void Update(Value value)
		{
			client.Execute(policy, key, PackageName, "update", binName, value, userModule);
		}

		/// <summary>
		/// Update/Add each value in array depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "update_all", binName, Value.Get(values), userModule);
		}

		/// <summary>
		/// Update/Add each value in values list depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update(IList values)
		{
			client.Execute(policy, key, PackageName, "update_all", binName, Value.GetAsList(values), userModule);
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
			client.Execute(policy, key, PackageName, "remove_all", binName, Value.GetAsList(values));
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
		/// Select range of values from list.
		/// </summary>
		/// <param name="begin">begin value inclusive</param>
		/// <param name="end">end value inclusive</param>
		public IList Range(Value begin, Value end)
		{
			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end);
		}
		
		/// <summary>
		/// Select values from list and apply specified Lua filter.
		/// </summary>
		/// <param name="value">value to select</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindThenFilter(Value value, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_then_filter", binName, value, userModule, Value.Get(filterName), Value.Get(filterArgs));
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
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter(string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "filter", binName, userModule, Value.Get(filterName), Value.Get(filterArgs));
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
		/// Set maximum number of entries in the list.
		/// </summary>
		/// <param name="capacity">max entries</param>
		public void SetCapacity(int capacity)
		{
			client.Execute(policy, key, PackageName, "set_capacity", binName, Value.Get(capacity));
		}

		/// <summary>
		/// Return maximum number of entries in the list.
		/// </summary>
		public int GetCapacity()
		{
			object result = client.Execute(policy, key, PackageName, "get_capacity", binName);
			return (result != null) ? (int)(long)result : 0;
		}
	}
}
