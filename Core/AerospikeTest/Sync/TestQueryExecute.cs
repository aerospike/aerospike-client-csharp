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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	public class QueryExecuteInit : TestSync, IDisposable
	{
		public QueryExecuteInit()
		{
			Assembly assembly = typeof(QueryExecuteInit).GetTypeInfo().Assembly;
			RegisterTask rtask = client.Register(null, assembly, "AerospikeTest.record_example.lua", "record_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.
			IndexTask itask = client.CreateIndex(policy, args.ns, args.set, TestQueryExecute.indexName, TestQueryExecute.binName1, IndexType.NUMERIC);
			itask.Wait();

			for (int i = 1; i <= TestQueryExecute.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestQueryExecute.keyPrefix + i);
				client.Put(null, key, new Bin(TestQueryExecute.binName1, i), new Bin(TestQueryExecute.binName2, i));
			}
		}

		public void Dispose()
		{
			client.DropIndex(null, args.ns, args.set, TestQueryExecute.indexName);
		}
	}

	public class TestQueryExecute : TestSync, Xunit.IClassFixture<QueryExecuteInit>
	{
		public const string indexName = "qeindex1";
		public const string keyPrefix = "qekey";
		public static readonly string binName1 = args.GetBinName("qebin1");
		public static readonly string binName2 = args.GetBinName("qebin2");
		public const int size = 10;

		QueryExecuteInit fixture;

		public TestQueryExecute(QueryExecuteInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void QueryExecute()
		{
			int begin = 3;
			int end = 9;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilters(Filter.Range(binName1, begin, end));

			ExecuteTask task = client.Execute(null, stmt, "record_example", "processRecord", Value.Get(binName1), Value.Get(binName2), Value.Get(100));
			task.Wait();
			ValidateRecords();
		}

		private void ValidateRecords()
		{
			int begin = 1;
			int end = size + 100;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilters(Filter.Range(binName1, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int[] expectedList = new int[] {1,2,3,104,5,106,7,108,-1,10};
				int expectedSize = size - 1;
				int count = 0;

				while (rs.Next())
				{
					Record record = rs.Record;
					int value1 = record.GetInt(binName1);
					int value2 = record.GetInt(binName2);

					int val1 = value1;

					if (val1 == 9)
					{
						Fail("Data mismatch. value1 " + val1 + " should not exist");
					}

					if (val1 == 5)
					{
						if (value2 != 0)
						{
							Fail("Data mismatch. value2 " + value2 + " should be null");
						}
					}
					else if (value1 != expectedList[value2 - 1])
					{
						Fail("Data mismatch. Expected " + expectedList[value2 - 1] + ". Received " + value1);
					}
					count++;
				}
				Xunit.Assert.Equal(expectedSize, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
