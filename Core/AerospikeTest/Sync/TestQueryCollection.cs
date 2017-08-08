/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Test
{
	public class QueryCollectionInit : TestSync, IDisposable
	{
		public QueryCollectionInit()
		{
			Assembly assembly = typeof(QueryCollectionInit).GetTypeInfo().Assembly;
			RegisterTask rtask = client.Register(null, assembly, "AerospikeTest.record_example.lua", "record_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, TestQueryCollection.indexName, TestQueryCollection.binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
			task.Wait();

			for (int i = 1; i <= TestQueryCollection.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestQueryCollection.keyPrefix + i);
				Dictionary<string, string> map = new Dictionary<string, string>();

				map[TestQueryCollection.mapKeyPrefix + 1] = TestQueryCollection.mapValuePrefix + i;
				if (i % 2 == 0)
				{
					map[TestQueryCollection.mapKeyPrefix + 2] = TestQueryCollection.mapValuePrefix + i;
				}
				if (i % 3 == 0)
				{
					map[TestQueryCollection.mapKeyPrefix + 3] = TestQueryCollection.mapValuePrefix + i;
				}

				Bin bin = new Bin(TestQueryCollection.binName, map);
				client.Put(null, key, bin);
			}
		}

		public void Dispose()
		{
			client.DropIndex(null, args.ns, args.set, TestQueryCollection.indexName);
		}
	}

	public class TestQueryCollection : TestSync, Xunit.IClassFixture<QueryCollectionInit>
	{
		public const string indexName = "mapkey_index";
		public const string keyPrefix = "qkey";
		public const string mapKeyPrefix = "mkey";
		public const string mapValuePrefix = "qvalue";
		public static readonly string binName = args.GetBinName("map_bin");
		public const int size = 20;

		QueryCollectionInit fixture;

		public TestQueryCollection(QueryCollectionInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
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
						Fail("Query mismatch: Expected mapKey " + queryMapKey + " Received " + result);
					}
					count++;
				}
				Xunit.Assert.NotEqual(0, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
