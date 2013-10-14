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
	/// Create and manage a map within a single bin.
	/// </summary>
	public sealed class LargeMap
	{
		private const string PackageName = "lmap";

		private readonly AerospikeClient client;
		private readonly Policy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value userModule;

		/// <summary>
		/// Initialize large map operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeMap(AerospikeClient client, Policy policy, Key key, string binName, string userModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.userModule = Value.Get(userModule);
		}

		/// <summary>
		/// Add entry to map.  If the map does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="name">entry key</param>
		/// <param name="value">entry value</param>
		public void Put(Value name, Value value)
		{
			client.Execute(policy, key, PackageName, "put", binName, name, value, userModule);
		}

		/// <summary>
		/// Add map values to map.  If the map does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="map">map values to push</param>
		public void Put(Dictionary<object,object> map)
		{
			client.Execute(policy, key, PackageName, "put_all", binName, Value.GetAsMap(map), userModule);
		}

		/// <summary>
		/// Get value from map given name key.
		/// </summary>
		/// <param name="name">key</param>
		public Dictionary<object,object> Get(Value name)
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "get", binName, name);
		}

		/// <summary>
		/// Return all objects in the map.
		/// </summary>
		public Dictionary<object,object> Scan()
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select items from map.
		/// </summary>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public Dictionary<object,object> Filter(string filterName, params Value[] filterArgs)
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "filter", binName, userModule, Value.Get(filterName), Value.Get(filterArgs));
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
			return (int)(long)client.Execute(policy, key, PackageName, "size", binName);
		}

		/// <summary>
		/// Return map configuration parameters.
		/// </summary>
		public Dictionary<object,object> GetConfig()
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "config", binName);
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
			return (int)(long)client.Execute(policy, key, PackageName, "get_capacity", binName);
		}
	}
}