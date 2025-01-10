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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryAverage : TestSync
	{
		private const string indexName = "avgindex";
		private const string keyPrefix = "avgkey";
		private static readonly string binName = Suite.GetBinName("l2");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.average_example.lua", "average_example.lua", Language.LUA);
			task.Wait();

			Policy policy = new();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.NUMERIC);
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
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new("l1", i);
				client.Put(null, key, bin, new Bin("l2", 1));
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryAverage()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName, 0, 1000));
			stmt.SetAggregateFunction(Assembly.GetExecutingAssembly(), "Aerospike.Test.LuaResources.average_example.lua", "average_example", "average");

			ResultSet rs = client.QueryAggregate(null, stmt);

			try
			{
				if (rs.Next())
				{
					object obj = rs.Object;

					if (obj is IDictionary)
					{
						IDictionary map = (IDictionary)obj;
						long sum = (long)map["sum"];
						long count = (long)map["count"];
						double avg = (double)sum / count;
						Assert.AreEqual(5.5, avg, 0.00000001);
					}
					else
					{
						Assert.Fail("Unexpected object returned: " + obj);
					}
				}
				else
				{
					Assert.Fail("Query Assert.Failed. No records returned.");
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
