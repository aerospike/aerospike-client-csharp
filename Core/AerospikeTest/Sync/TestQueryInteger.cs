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
using Aerospike.Client;

namespace Aerospike.Test
{
	public class QueryIntegerInit : TestSync, IDisposable
	{
		public QueryIntegerInit()
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, TestQueryInteger.indexName, TestQueryInteger.binName, IndexType.NUMERIC);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= TestQueryInteger.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestQueryInteger.keyPrefix + i);
				Bin bin = new Bin(TestQueryInteger.binName, i);
				client.Put(null, key, bin);
			}
		}

		public void Dispose()
		{
			client.DropIndex(null, args.ns, args.set, TestQueryInteger.indexName);
		}
	}

	public class TestQueryInteger : TestSync, Xunit.IClassFixture<QueryIntegerInit>
	{
		public const string indexName = "queryindexint";
		public const string keyPrefix = "querykeyint";
		public static readonly string binName = args.GetBinName("querybinint");
		public const int size = 50;

		QueryIntegerInit fixture;

		public TestQueryInteger(QueryIntegerInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void QueryInteger()
		{
			int begin = 14;
			int end = 18;

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
					count++;
				}
				Xunit.Assert.Equal(5, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
