/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQuerySum : TestSync
	{
		private const string indexName = "aggindex";
		private const string keyPrefix = "aggkey";
		private static readonly string binName = args.GetBinName("aggbin");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.sum_example.lua", "sum_example.lua", Language.LUA);
			task.Wait();

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask itask = nativeClient.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			nativeClient.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QuerySum()
		{
			int begin = 4;
			int end = 7;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetAggregateFunction(Assembly.GetExecutingAssembly(), "Aerospike.Test.LuaResources.sum_example.lua", "sum_example", "sum_single_bin", Value.Get(binName));

			ResultSet rs = nativeClient.QueryAggregate(null, stmt);

			try
			{
				int expected = 22; // 4 + 5 + 6 + 7
				int count = 0;

				while (rs.Next())
				{
					object obj = rs.Object;
					long sum = 0;

					if (obj is long)
					{
						sum = (long)rs.Object;
					}
					else
					{
						Assert.Fail("Return value not a long: " + obj);
					}
					Assert.AreEqual(expected, (int)sum);
					count++;
				}
				Assert.AreNotEqual(0, count);
			}
			finally
			{
				rs.Close();
			}
		}

		[TestMethod]
		public void QuerySetNotFound()
		{
			Statement stmt = new Statement()
			{
				Namespace = args.ns,
				SetName = "notfound",
				BinNames = new string[] { binName },
				Filter = Filter.Range(binName, 4, 7)
			};
			stmt.SetAggregateFunction(Assembly.GetExecutingAssembly(), "Aerospike.Test.LuaResources.sum_example.lua", "sum_example", "sum_single_bin", Value.Get(binName));

			QueryPolicy qp = new QueryPolicy()
			{
				socketTimeout = 5000
			};

			ResultSet rs = nativeClient.QueryAggregate(qp, stmt);

			try
			{
				while (rs.Next())
				{
					Assert.Fail("No rows should have been returned");
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
