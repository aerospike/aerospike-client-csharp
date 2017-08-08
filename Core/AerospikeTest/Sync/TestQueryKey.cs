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
using Aerospike.Client;

namespace Aerospike.Test
{
	public class QueryKeyInit : TestSync, IDisposable
	{
		public QueryKeyInit()
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.
			IndexTask itask = client.CreateIndex(policy, args.ns, args.set, TestQueryKey.indexName, TestQueryKey.binName, IndexType.NUMERIC);
			itask.Wait();

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.sendKey = true;

			for (int i = 1; i <= TestQueryKey.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestQueryKey.keyPrefix + i);
				Bin bin = new Bin(TestQueryKey.binName, i);
				client.Put(writePolicy, key, bin);
			}
		}

		public void Dispose()
		{
			client.DropIndex(null, args.ns, args.set, TestQueryKey.indexName);
		}
	}

	public class TestQueryKey : TestSync, Xunit.IClassFixture<QueryKeyInit>
	{
		public const string indexName = "skindex";
		public const string keyPrefix = "skkey";
		public static readonly string binName = args.GetBinName("skbin");
		public const int size = 10;

		QueryKeyInit fixture;

		public TestQueryKey(QueryKeyInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void QueryKey()
		{
			int begin = 2;
			int end = 5;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Range(binName, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Xunit.Assert.NotNull(key.userKey);

					object userkey = key.userKey.Object;
					Xunit.Assert.NotNull(userkey);
					count++;
				}
				Xunit.Assert.Equal(4, count);
			}
			finally
			{
				rs.Close();
			}
		}	
	}
}
