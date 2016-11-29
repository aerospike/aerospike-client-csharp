/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	/// Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
	/// <para>
	/// Deprecated: LDT functionality has been deprecated.
	/// </para>
	/// </summary>
	public sealed class LargeStack
	{
		private const string PackageName = "lstack";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly Value createModule;

		/// <summary>
		/// Initialize large stack operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="createModule">Lua function name that initializes list configuration parameters, pass null for default set</param>
		public LargeStack(AerospikeClient client, WritePolicy policy, Key key, string binName, string createModule)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.createModule = Value.Get(createModule);
		}

		/// <summary>
		/// Push value onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to push</param>
		public void Push(Value value)
		{
			client.Execute(policy, key, PackageName, "push", binName, value, createModule);
		}

		/// <summary>
		/// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to push</param>
		public void Push(params Value[] values)
		{
			client.Execute(policy, key, PackageName, "push_all", binName, Value.Get(values), createModule);
		}

		/// <summary>
		/// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to push</param>
		public void Push(IList values)
		{
			client.Execute(policy, key, PackageName, "push_all", binName, Value.Get(values), createModule);
		}
		
		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select</param>
		public IList Peek(int peekCount)
		{
			return (IList)client.Execute(policy, key, PackageName, "peek", binName, Value.Get(peekCount));
		}

		/// <summary>
		/// Return list of all objects on the stack.
		/// </summary>
		public IList Scan()
		{
			return (IList)client.Execute(policy, key, PackageName, "scan", binName);
		}

		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select.</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter(int peekCount, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.Get(peekCount), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
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
			object result = client.Execute(policy, key, PackageName, "size", binName);
			return (result != null) ? (int)(long)result : 0;
		}

		/// <summary>
		/// Return map of stack configuration parameters.
		/// </summary>
		public IDictionary GetConfig()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "get_config", binName);
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
			object result = client.Execute(policy, key, PackageName, "get_capacity", binName);
			return (result != null) ? (int)(long)result : 0;
		}
	}
}
