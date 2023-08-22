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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryAverage : TestSync
	{
		private const string indexName = "avgindex";
		private const string keyPrefix = "avgkey";
		private static readonly string binName = args.GetBinName("l2");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.average_example.lua", "average_example.lua", Language.LUA);
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
				Bin bin = new Bin("l1", i);
				client.Put(null, key, bin, new Bin("l2", 1));
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			nativeClient.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QueryAverage()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName, 0, 1000));
			stmt.SetAggregateFunction(Assembly.GetExecutingAssembly(), "Aerospike.Test.LuaResources.average_example.lua", "average_example", "average");

			ResultSet rs = nativeClient.QueryAggregate(null, stmt);

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
						double avg = (double) sum / count;
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
