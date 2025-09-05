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
	public class TestQueryCollection : TestSync
	{
		private const string indexName = "mapkey_index";
		private const string keyPrefix = "qkey";
		private const string mapKeyPrefix = "mkey";
		private const string mapValuePrefix = "qvalue";
		private static readonly string binName = Suite.GetBinName("map_bin");
		private const int size = 20;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask rtask = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask task = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
				task.Wait();
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
				Dictionary<string, string> map = new()
				{
					[mapKeyPrefix + 1] = mapValuePrefix + i
				};
				if (i % 2 == 0)
				{
					map[mapKeyPrefix + 2] = mapValuePrefix + i;
				}
				if (i % 3 == 0)
				{
					map[mapKeyPrefix + 3] = mapValuePrefix + i;
				}

				Bin bin = new(binName, map);
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryCollection()
		{
			string queryMapKey = mapKeyPrefix + 2;
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Contains(binName, IndexCollectionType.MAPKEYS, queryMapKey));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Record record = rs.Record;
					IDictionary result = (IDictionary)record.GetValue(binName);

					if (!result.Contains(queryMapKey))
					{
						Assert.Fail("Query mismatch: Expected mapKey " + queryMapKey + " Received " + result);
					}
					count++;
				}
				Assert.AreNotEqual(0, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
