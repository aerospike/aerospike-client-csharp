/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Aerospike.Client;
using System.Threading;

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
			Register(client, args);
			WriteUsingUdfAsync(client, args);
			WaitTillComplete();
			completed = false;
		}

		private void Register(IAerospikeClient client, Arguments args)
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
			client.Execute(null, new WriteHandler(this, client, key, bin.name), key, "record_example", "writeBin", Value.Get(bin.name), bin.value);
		}

		private class WriteHandler : ExecuteListener
		{
			private readonly AsyncUserDefinedFunction parent;
			private readonly AsyncClient client;
			private readonly Key key;
			private readonly string binName;

			public WriteHandler(AsyncUserDefinedFunction parent, AsyncClient client, Key key, string binName)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.binName = binName;
			}

			public void OnSuccess(Key key, object obj)
			{
				// Write succeeded.  Now call read using udf.
				parent.console.Info("Read with udf: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey);
				client.Execute(null, new ReadHandler(parent, key), key, "record_example", "readBin", Value.Get(binName));
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to put: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler : ExecuteListener
		{
			private readonly AsyncUserDefinedFunction parent;
			private Key key;

			public ReadHandler(AsyncUserDefinedFunction parent, Key key)
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
				parent.console.Error("Failed to get: namespace={0} set={1} key={2} exception={3}",
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
