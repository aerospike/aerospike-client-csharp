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
	public class QueryStringInit : TestSync, IDisposable
	{
		public QueryStringInit()
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, TestQueryString.indexName, TestQueryString.binName, IndexType.STRING);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= TestQueryString.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestQueryString.keyPrefix + i);
				Bin bin = new Bin(TestQueryString.binName, TestQueryString.valuePrefix + i);
				client.Put(null, key, bin);
			}
		}

		public void Dispose()
		{
			client.DropIndex(null, args.ns, args.set, TestQueryString.indexName);
		}
	}

	public class TestQueryString : TestSync, Xunit.IClassFixture<QueryStringInit>
	{
		public const string indexName = "queryindex";
		public const string keyPrefix = "querykey";
		public const string valuePrefix = "queryvalue";
		public static readonly string binName = args.GetBinName("querybin");
		public static int size = 5;

		QueryStringInit fixture;

		public TestQueryString(QueryStringInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void QueryString()
		{
			string filter = valuePrefix + 3;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Equal(binName, filter));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Record record = rs.Record;
					string result = record.GetString(binName);
					Xunit.Assert.Equal(filter, result);
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
