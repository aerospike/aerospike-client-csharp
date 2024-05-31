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
		private static readonly string binName = args.GetBinName("map_bin");
		private const int size = 20;

		[ClassInitialize()]
		public static async Task Prepare(TestContext testContext)
		{
			if ((!args.testProxy && !args.testAsyncAwait) || (args.testProxy && nativeClient != null))
			{
				Assembly assembly = Assembly.GetExecutingAssembly();
				RegisterTask rtask = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
				rtask.Wait();
			}
			else if (args.testAsyncAwait)
			{
				throw new NotImplementedException();
			}

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				if ((!args.testProxy && !args.testAsyncAwait) || (args.testProxy && nativeClient != null))
				{
					IndexTask task = nativeClient.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
					task.Wait();
				}
				else if (args.testAsyncAwait)
				{ 
					throw new NotImplementedException(); 
				}
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
				Dictionary<string, string> map = new Dictionary<string, string>();

				map[mapKeyPrefix + 1] = mapValuePrefix + i;
				if (i % 2 == 0)
				{
					map[mapKeyPrefix + 2] = mapValuePrefix + i;
				}
				if (i % 3 == 0)
				{
					map[mapKeyPrefix + 3] = mapValuePrefix + i;
				}

				Bin bin = new Bin(binName, map);
				if (!args.testAsyncAwait)
				{
					client.Put(null, key, bin);
				}
				else
				{
					await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);
				}
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			if ((!args.testProxy && !args.testAsyncAwait) || (args.testProxy && nativeClient != null))
			{
				nativeClient.DropIndex(null, args.ns, args.set, indexName);
			}
			else if (args.testAsyncAwait)
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryCollection()
		{
			string queryMapKey = mapKeyPrefix + 2;
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Contains(binName, IndexCollectionType.MAPKEYS, queryMapKey));

			if (!args.testAsyncAwait)
			{
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
			else if (args.testAsyncAwait)
			{
				throw new NotImplementedException();
			}
		}
	}
}
