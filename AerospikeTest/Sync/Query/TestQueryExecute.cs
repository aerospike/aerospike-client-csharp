﻿/* 
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
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryExecute : TestSync
	{
		private const string indexName = "tqeindex";
		private const string keyPrefix = "tqekey";
		private static readonly string binName1 = args.GetBinName("tqebin1");
		private static readonly string binName2 = args.GetBinName("tqebin2");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
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
					IndexTask itask = nativeClient.CreateIndex(policy, args.ns, args.set, indexName, binName1, IndexType.NUMERIC);
					itask.Wait();
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

		[TestInitialize()]
		public async Task InitializeTest()
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				if (!args.testAsyncAwait)
				{
					client.Put(null, key, new Bin(binName1, i), new Bin(binName2, i));
				}
				else
				{
					await asyncAwaitClient.Put(null, key, new[] { new Bin(binName1, i), new Bin(binName2, i) }, CancellationToken.None);
				}
			}
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

			if (!args.testAsyncAwait)
			{
				ExecuteTask task = client.Execute(null, stmt, "record_example", "processRecord", Value.Get(binName1), Value.Get(binName2), Value.Get(100));
				task.Wait(3000, 3000);
				ValidateRecords();
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		private void ValidateRecords()
		{
			int begin = 1;
			int end = size + 100;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(null, stmt);

				try
				{
					int[] expectedList = new int[] { 1, 2, 3, 104, 5, 106, 7, 108, -1, 10 };
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
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryExecuteOperate()
		{
			int begin = 3;
			int end = 9;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Bin bin = new Bin("foo", "bar");

			if (!args.testAsyncAwait)
			{
				ExecuteTask task = client.Execute(null, stmt, Operation.Put(bin));
				task.Wait(3000, 3000);

				string expected = bin.value.ToString();

				stmt = new Statement();
				stmt.SetNamespace(args.ns);
				stmt.SetSetName(args.set);
				stmt.SetFilter(Filter.Range(binName1, begin, end));

				RecordSet rs = client.Query(null, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						Record record = rs.Record;
						string value = record.GetString(bin.name);

						if (value == null)
						{
							Assert.Fail("Bin " + bin.name + " not found");
						}

						if (!value.Equals(expected))
						{
							Assert.Fail("Data mismatch. Expected " + expected + ". Received " + value);
						}
						count++;
					}
					Assert.AreEqual(end - begin + 1, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryExecuteOperateExp()
		{
			string binName = "foo";
			Expression exp = Exp.Build(Exp.Val("bar"));

			int begin = 3;
			int end = 9;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			if (!args.testAsyncAwait)
			{
				ExecuteTask task = client.Execute(null, stmt,
					ExpOperation.Write(binName, exp, ExpWriteFlags.DEFAULT)
					);

				task.Wait(3000, 3000);

				stmt = new Statement();
				stmt.SetNamespace(args.ns);
				stmt.SetSetName(args.set);
				stmt.SetFilter(Filter.Range(binName1, begin, end));

				RecordSet rs = client.Query(null, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						Record record = rs.Record;
						string value = record.GetString(binName);

						if (value == null)
						{
							Assert.Fail("Bin " + binName + " not found");
						}

						if (!value.Equals("bar"))
						{
							Assert.Fail("Data mismatch. Expected bar. Received " + value);
						}
						count++;
					}
					Assert.AreEqual(end - begin + 1, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryExecuteSetNotFound()
		{
			Statement stmt = new Statement
			{
				Namespace = args.ns,
				SetName = "notfound",
				Filter = Filter.Range(binName1, 1, 3)
			};

			if (!args.testAsyncAwait)
			{
				// Previous client versions might timeout when set does not exist.
				// Test to make sure regression has not resurfaced.
				client.Execute(null, stmt, Operation.Touch());
			}
			else
			{
				throw new NotImplementedException();
			}
		}
	}
}
