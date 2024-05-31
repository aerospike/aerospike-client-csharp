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
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryFilterExp : TestSync
	{
		private static readonly string setName = args.set + "flt";
		private const string indexName = "flt";
		private const string keyPrefix = "flt";
		private const string binName = "fltint";
		private const int size = 50;

		[ClassInitialize()]
		public static async Task Prepare(TestContext testContext)
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				if ((!args.testProxy && !args.testAsyncAwait) || (args.testProxy && nativeClient != null))
				{
					IndexTask itask = nativeClient.CreateIndex(policy, args.ns, setName, indexName, binName, IndexType.NUMERIC);
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

			// Write records with string keys
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, setName, keyPrefix + i);
				List<int> list = null;
				Dictionary<string, string> map = null;

				if (i == 1)
				{
					list = new List<int>(5);
					list.Add(1);
					list.Add(2);
					list.Add(4);
					list.Add(9);
					list.Add(20);
					// map will be null, which means mapbin will not exist in this record.
				}
				else if (i == 2)
				{
					list = new List<int>(3);
					list.Add(5);
					list.Add(9);
					list.Add(100);
					// map will be null, which means mapbin will not exist in this record.
				}
				else if (i == 3)
				{
					map = new Dictionary<string, string>();
					map["A"] = "AAA";
					map["B"] = "BBB";
					map["C"] = "BBB";
					// list will be null, which means listbin will not exist in this record.
				}
				else
				{
					list = new List<int>(0);
					map = new Dictionary<string, string>(0);
				}
				if (!args.testAsyncAwait)
				{
					client.Put(null, key, new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map));
				}
				else
				{
					await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map) }, CancellationToken.None);
				}
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			if ((!args.testProxy && !args.testAsyncAwait) || (args.testProxy && nativeClient != null))
			{
				nativeClient.DropIndex(null, args.ns, setName, indexName);
			}
			else if (args.testProxy)
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryAndOr()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// ((bin2 > 40 && bin2 < 44) || bin2 == 22 || bin2 == 9) && (binName == bin2)
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.Or(
						Exp.And(
							Exp.GT(Exp.IntBin("bin2"), Exp.Val(40)),
							Exp.LT(Exp.IntBin("bin2"), Exp.Val(44))),
						Exp.EQ(Exp.IntBin("bin2"), Exp.Val(22)),
						Exp.EQ(Exp.IntBin("bin2"), Exp.Val(9))),
					Exp.EQ(Exp.IntBin(binName), Exp.IntBin("bin2"))));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.GetValue(binName));
						count++;
					}
					// 22, 41, 42, 43
					Assert.AreEqual(4, count);
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
		public void QueryNot()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// ! (bin2 >= 15 && bin2 <= 42)
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.And(
						Exp.GE(Exp.IntBin("bin2"), Exp.Val(15)),
						Exp.LE(Exp.IntBin("bin2"), Exp.Val(42)))));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.GetValue(binName));
						count++;
					}
					// 10, 11, 12, 13, 43, 44, 45
					Assert.AreEqual(8, count);
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
		public void QueryLastUpdate()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// record last update time > (currentTimeMillis() * 1000000L + 100)
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.GT(
					Exp.LastUpdate(),
					Exp.Val(DateTime.UtcNow.Add(TimeSpan.FromSeconds(1.0)))));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					//int count = 0;

					while (rs.Next())
					{
						//Record record = rs.Record;
						//Console.WriteLine(record.GetValue(binName).ToString() + ' ' + record.expiration);
						//count++;
					}
					// Do not asset count since some tests can run after this one.
					//Assert.AreEqual(0, count);
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
		public void QueryList1()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// List bin contains at least one integer item == 4
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.GT(
					ListExp.GetByValue(ListReturnType.COUNT, Exp.Val(4), Exp.ListBin("listbin")),
					Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryList2()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// List bin does not contain integer item == 5
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.GetByValue(ListReturnType.COUNT, Exp.Val(5), Exp.ListBin("listbin")),
					Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(8, count);
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
		public void QueryList3()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// list[4] == 20
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(4), Exp.ListBin("listbin")),
					Exp.Val(20)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryMap1()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains key "B"
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.GT(
					MapExp.GetByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.Val("B"), Exp.MapBin("mapbin")),
					Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryMap2()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains value "BBB"
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				MapExp.GetByValue(MapReturnType.EXISTS, Exp.Val("BBB"), Exp.MapBin("mapbin")));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryMap3()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin does not contains key "D"
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					MapExp.GetByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.Val("D"), Exp.MapBin("mapbin")),
					Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(8, count);
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
		public void QueryMap4()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin does not contains value "AAA"
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					MapExp.GetByValue(MapReturnType.COUNT, Exp.Val("AAA"), Exp.MapBin("mapbin")),
					Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(7, count);
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
		public void QueryMap5()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains keys "A" and "C".
			QueryPolicy policy = new QueryPolicy();

			List<string> list = new List<string>();
			list.Add("A");
			list.Add("C");

			policy.filterExp = Exp.Build(
				Exp.EQ(
					MapExp.Size(
						MapExp.GetByKeyList(MapReturnType.KEY_VALUE, Exp.Val(list), Exp.MapBin("mapbin"))),
					Exp.Val(2)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryMap6()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains keys "A" and "C".
			QueryPolicy policy = new QueryPolicy();

			List<string> list = new List<string>();
			list.Add("A");
			list.Add("C");

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size( // return type VALUE returns a list
						MapExp.GetByKeyList(MapReturnType.VALUE, Exp.Val(list), Exp.MapBin("mapbin"))),
					Exp.Val(2)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(1, count);
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
		public void QueryDigestModulo()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Record key digest % 3 == 1
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.DigestModulo(3), Exp.Val(1)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//Console.WriteLine(rs.Record.ToString());
						count++;
					}
					Assert.AreEqual(4, count);
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
		public void QueryBinExists()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.BinExists("bin2"));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(10, count);
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
		public void QueryBinType()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.BinType("listbin"), Exp.Val((int)ParticleType.LIST)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(9, count);
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
		public void QueryRecordSize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// This just tests that the expression was sent correctly
			// because all record sizes are effectively allowed
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.GE(Exp.RecordSize(), Exp.Val(0)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(10, count);
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
		public void QueryDeviceSize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// storage-engine could be memory for which DeviceSize() returns zero.
			// This just tests that the expression was sent correctly
			// because all device sizes are effectively allowed.
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.GE(Exp.DeviceSize(), Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(10, count);
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
		public void QueryMemorySize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// storage-engine could be memory for which MemorySize() returns zero.
			// This just tests that the expression was sent correctly
			// because all memory sizes are effectively allowed.
			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.GE(Exp.MemorySize(), Exp.Val(0)));
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(10, count);
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
	}
}
