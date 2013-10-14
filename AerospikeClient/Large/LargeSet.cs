/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
		private readonly Policy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value userModule;

		/// <summary>
		/// Initialize large set operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeSet(AerospikeClient client, Policy policy, Key key, string binName, string userModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.userModule = Value.Get(userModule);
		}

		/// <summary>
		/// Add a value to the set.  If the set does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to add</param>
		public void Add(Value value)
		{
			client.Execute(policy, key, PackageName, "add", binName, value, userModule);
		}

		/// <summary>
		/// Add values to the set.  If the set does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "add_all", binName, Value.Get(values), userModule);
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
			int ret = (int)client.Execute(policy, key, PackageName, "exists", binName, value);
			return ret == 1;
		}

		/// <summary>
		/// Return list of all objects in the set.
		/// </summary>
		public List<object> Scan()
		{
			return (List<object>)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select values from set and apply specified Lua filter.
		/// </summary>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public List<object> Filter(string filterName, params Value[] filterArgs)
		{
			return (List<object>)client.Execute(policy, key, PackageName, "filter", binName, userModule, Value.Get(filterName), Value.Get(filterArgs));
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
			return (int)(long)client.Execute(policy, key, PackageName, "size", binName);
		}

		/// <summary>
		/// Return map of set configuration parameters.
		/// </summary>
		public Dictionary<object,object> GetConfig()
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "get_config", binName);
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
			return (int)(long)client.Execute(policy, key, PackageName, "get_capacity", binName);
		}
	}
}