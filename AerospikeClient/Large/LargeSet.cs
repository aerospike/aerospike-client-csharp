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
	public sealed class LargeSet
	{
		private const string PackageName = "lset";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value createModule;

		/// <summary>
		/// Initialize large set operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="createModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeSet(AerospikeClient client, WritePolicy policy, Key key, string binName, string createModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.createModule = Value.Get(createModule);
		}

		/// <summary>
		/// Add a value to the set.  If the set does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to add</param>
		public void Add(Value value)
		{
			client.Execute(policy, key, PackageName, "add", binName, value, createModule);
		}

		/// <summary>
		/// Add values to the set.  If the set does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.Get(values), createModule);
		}

		/// <summary>
		/// Add values to the list.  If the list does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(IList values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.GetAsList(values), createModule);
		}

		/// <summary>
		/// Delete value from set.
		/// </summary>
		/// <param name="value">value to delete</param>
		public void Remove(Value value)
		{
			client.Execute(policy, key, PackageName, "remove", binName, value);
		}

		/// <summary>
		/// Select value from set.
		/// </summary>
		/// <param name="value">value to select</param>
		public object Get(Value value)
		{
			return client.Execute(policy, key, PackageName, "get", binName, value);
		}

		/// <summary>
		/// Check existence of value in the set.
		/// </summary>
		/// <param name="value">value to check</param>
		public bool Exists(Value value)
		{
			object result = client.Execute(policy, key, PackageName, "exists", binName, value);
			return (result != null) ? ((long)result != 0) : false;
		}

		/// <summary>
		/// Return list of all objects in the set.
		/// </summary>
		public IList Scan()
		{
			return (IList)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select values from set and apply specified Lua filter.
		/// </summary>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter(string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Delete bin containing the set.
		/// </summary>
		public void Destroy()
		{
			client.Execute(policy, key, PackageName, "destroy", binName);
		}

		/// <summary>
		/// Return size of set.
		/// </summary>
		public int Size()
		{
			object result = client.Execute(policy, key, PackageName, "size", binName);
			return (result != null) ? (int)(long)result : 0;
		}

		/// <summary>
		/// Return map of set configuration parameters.
		/// </summary>
		public IDictionary GetConfig()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "get_config", binName);
		}

		/// <summary>
		/// Set maximum number of entries in the set.
		/// </summary>
		/// <param name="capacity">max entries in set </param>
		public void SetCapacity(int capacity)
		{
			client.Execute(policy, key, PackageName, "set_capacity", binName, Value.Get(capacity));
		}

		/// <summary>
		/// Return maximum number of entries in the set.
		/// </summary>
		public int GetCapacity()
		{
			object result = client.Execute(policy, key, PackageName, "get_capacity", binName);
			return (result != null) ? (int)(long)result : 0;
		}
	}
}
