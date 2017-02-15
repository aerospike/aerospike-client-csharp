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
	public class TestQueryExecute : TestSync
	{
		private const string indexName = "qeindex1";
		private const string keyPrefix = "qekey";
		private static readonly string binName1 = args.GetBinName("qebin1");
		private static readonly string binName2 = args.GetBinName("qebin2");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask rtask = client.Register(null, assembly, "Aerospike.Test.Resources.record_example.lua", "record_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask itask = client.CreateIndex(policy, args.ns, args.set, indexName, binName1, IndexType.NUMERIC);
			itask.Wait();

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				client.Put(null, key, new Bin(binName1, i), new Bin(binName2, i));
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QueryExecute()
		{
			int begin = 3;
			int end = 9;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

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
			stmt.SetFilter(Filter.Range(binName1, begin, end));

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
						Assert.Fail("Data mismatch. value1 " + val1 + " should not exist");
					}

					if (val1 == 5)
					{
						if (value2 != 0)
						{
							Assert.Fail("Data mismatch. value2 " + value2 + " should be null");
						}
					}
					else if (value1 != expectedList[value2 - 1])
					{
						Assert.Fail("Data mismatch. Expected " + expectedList[value2 - 1] + ". Received " + value1);
					}
					count++;
				}
				Assert.AreEqual(expectedSize, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
