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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

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
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask rtask = client.Register(null, assembly, "Aerospike.Test.Resources.record_example.lua", "record_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
			task.Wait();

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
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QueryCollection()
		{
			string queryMapKey = mapKeyPrefix + 2;
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Contains(binName, IndexCollectionType.MAPKEYS, queryMapKey));

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
