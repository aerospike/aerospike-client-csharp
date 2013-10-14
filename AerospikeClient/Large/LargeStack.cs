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
	/// Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
	/// </summary>
	public sealed class LargeStack
	{
		private const string PackageName = "lstack";

		private readonly AerospikeClient client;
		private readonly Policy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value userModule;

		/// <summary>
		/// Initialize large stack operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeStack(AerospikeClient client, Policy policy, Key key, string binName, string userModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.userModule = Value.Get(userModule);
		}

		/// <summary>
		/// Push value onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to push</param>
		public void Push(Value value)
		{
			client.Execute(policy, key, PackageName, "push", binName, value, userModule);
		}

		/// <summary>
		/// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to push</param>
		public void Push(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "push", binName, Value.Get(values), userModule);
		}

		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select</param>
		public List<object> Peek(int peekCount)
		{
			return (List<object>)client.Execute(policy, key, PackageName, "peek", binName, Value.Get(peekCount));
		}

		/// <summary>
		/// Return list of all objects on the stack.
		/// </summary>
		public List<object> Scan()
		{
			return (List<object>)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select.</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public List<object> Filter(int peekCount, string filterName, params Value[] filterArgs)
		{
			return (List<object>)client.Execute(policy, key, PackageName, "filter", binName, Value.Get(peekCount), Value.Get(filterName), Value.Get(filterArgs));
		}

		/// <summary>
		/// Delete bin containing the stack.
		/// </summary>
		public void Destroy()
		{
			client.Execute(policy, key, PackageName, "destroy", binName);
		}

		/// <summary>
		/// Return size of stack.
		/// </summary>
		public int Size()
		{
			return (int)(long)client.Execute(policy, key, PackageName, "size", binName);
		}

		/// <summary>
		/// Return map of stack configuration parameters.
		/// </summary>
		public Dictionary<object,object> GetConfig()
		{
			return (Dictionary<object,object>)client.Execute(policy, key, PackageName, "get_config", binName);
		}

		/// <summary>
		/// Set maximum number of entries for the stack.
		/// </summary>
		/// <param name="capacity">max entries</param>
		public void SetCapacity(int capacity)
		{
			client.Execute(policy, key, PackageName, "set_capacity", binName, Value.Get(capacity));
		}

		/// <summary>
		/// Return maximum number of entries for the stack.
		/// </summary>
		public int GetCapacity()
		{
			return (int)(long)client.Execute(policy, key, PackageName, "get_capacity", binName);
		}
	}
}