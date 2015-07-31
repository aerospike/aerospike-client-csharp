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
using System;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class AsyncUserDefinedFunction : AsyncExample
	{
		private bool completed;

		public AsyncUserDefinedFunction(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Asynchronous UDF example.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			if (! args.hasUdf) {
				console.Info("Execute functions are not supported by the connected Aerospike server.");
				return;
			}
		
			Register(client, args);
			WriteUsingUdfAsync(client, args);
			WaitTillComplete();
			completed = false;
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "record_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void WriteUsingUdfAsync(AsyncClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "audfkey1");
			Bin bin = new Bin(args.GetBinName("audfbin1"), "string value");

			console.Info("Write with udf: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " value=" + bin.value);
			client.Execute(args.writePolicy, new ExecuteHandler(this, key) , key, "record_example", "readBin", Value.Get(bin.name));
		}

		private class ExecuteHandler : ExecuteListener
		{
			private readonly AsyncUserDefinedFunction parent;
			private Key key;

			public ExecuteHandler(AsyncUserDefinedFunction parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, object obj)
			{
				parent.console.Info("Result: " + obj);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to put: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);

				parent.NotifyCompleted();
			}
		}
		
		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}
}
