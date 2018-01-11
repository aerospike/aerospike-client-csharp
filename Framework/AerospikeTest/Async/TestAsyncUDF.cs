/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncUDF : TestAsync
	{
		private static readonly string binName = args.GetBinName("audfbin1");
		private const string binValue = "string value";

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.Resources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestMethod]
		public void AsyncUDF()
		{
			Key key = new Key(args.ns, args.set, "audfkey1");
			Bin bin = new Bin(binName, binValue);

			// Write bin
			client.Execute(null, new WriteHandler(this, key), key, "record_example", "writeBin", Value.Get(bin.name), bin.value);
			WaitTillComplete();
		}

		private class WriteHandler : ExecuteListener
		{
			private readonly TestAsyncUDF parent;
			private Key key;

			public WriteHandler(TestAsyncUDF parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, object obj)
			{
				// Write succeeded.  Now call read using udf.
				client.Execute(null, new ReadHandler(parent, key), key, "record_example", "readBin", Value.Get(binName));
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler : ExecuteListener
		{
			private readonly TestAsyncUDF parent;
			private Key key;

			public ReadHandler(TestAsyncUDF parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, object received)
			{
				if (parent.AssertNotNull(received)) {
					parent.AssertEquals(binValue, received);
				}
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
