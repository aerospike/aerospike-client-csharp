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
	/// Create and manage a map within a single bin.
	/// </summary>
	public sealed class LargeMap
	{
		private const string PackageName = "lmap";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value createModule;

		/// <summary>
		/// Initialize large map operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="createModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeMap(AerospikeClient client, WritePolicy policy, Key key, string binName, string createModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.createModule = Value.Get(createModule);
		}

		/// <summary>
		/// Add entry to map.  If the map does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="name">entry key</param>
		/// <param name="value">entry value</param>
		public void Put(Value name, Value value)
		{
			client.Execute(policy, key, PackageName, "put", binName, name, value, createModule);
		}

		/// <summary>
		/// Add map values to map.  If the map does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="map">map values to push</param>
		public void Put(IDictionary map)
		{
			client.Execute(policy, key, PackageName, "put_all", binName, Value.Get(map), createModule);
		}

		/// <summary>
		/// Get value from map given name key.
		/// </summary>
		/// <param name="name">key</param>
		public IDictionary Get(Value name)
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "get", binName, name);
		}

		/// <summary>
		/// Check existence of key in the map.
		/// </summary>
		/// <param name="keyValue">key to check</param>
		public bool Exists(Value keyValue)
		{
			object result = client.Execute(policy, key, PackageName, "exists", binName, keyValue);
			return Util.ToBool(result);
		}
	
		/// <summary>
		/// Return all objects in the map.
		/// </summary>
		public IDictionary Scan()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select items from map.
		/// </summary>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IDictionary Filter(string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "filter", binName, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Remove entry from map.
		/// </summary>
		public void Remove(Value name)
		{
			client.Execute(policy, key, PackageName, "remove", binName, name, createModule);
		}

		/// <summary>
		/// Delete bin containing the map.
		/// </summary>
		public void Destroy()
		{
			client.Execute(policy, key, PackageName, "destroy", binName);
		}

		/// <summary>
		/// Return size of map.
		/// </summary>
		public int Size()
		{
			object result = client.Execute(policy, key, PackageName, "size", binName);
			return (result != null) ? (int)(long)result : 0;
		}

		/// <summary>
		/// Return map configuration parameters.
		/// </summary>
		public IDictionary GetConfig()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "config", binName);
		}

		/// <summary>
		/// Set maximum number of entries for the map.
		/// </summary>
		/// <param name="capacity">max entries</param>
		public void SetCapacity(int capacity)
		{
			client.Execute(policy, key, PackageName, "set_capacity", binName, Value.Get(capacity));
		}

		/// <summary>
		/// Return maximum number of entries for the map.
		/// </summary>
		public int GetCapacity()
		{
			object result = client.Execute(policy, key, PackageName, "get_capacity", binName);
			return (result != null) ? (int)(long)result : 0;
		}
	}
}
