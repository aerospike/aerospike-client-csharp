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

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryFilterExp : TestSync
	{
		private static readonly string setName = SuiteHelpers.set + "flt";
		private const string indexName = "flt";
		private const string keyPrefix = "flt";
		private const string binName = "fltint";
		private const int size = 50;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, setName, indexName, binName, IndexType.NUMERIC);
				itask.Wait();
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
				Key key = new(SuiteHelpers.ns, setName, keyPrefix + i);
				List<int> list = null;
				Dictionary<string, string> map = null;

				if (i == 1)
				{
					list = [1, 2, 4, 9, 20];
					// map will be null, which means mapbin will not exist in this record.
				}
				else if (i == 2)
				{
					list = [5, 9, 100];
					// map will be null, which means mapbin will not exist in this record.
				}
				else if (i == 3)
				{
					map = new Dictionary<string, string>
					{
						["A"] = "AAA",
						["B"] = "BBB",
						["C"] = "BBB"
					};
					// list will be null, which means listbin will not exist in this record.
				}
				else
				{
					list = [];
					map = [];
				}
				client.Put(null, key, new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map));
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, setName, indexName);
		}

		[TestMethod]
		public void QueryAndOr()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// ((bin2 > 40 && bin2 < 44) || bin2 == 22 || bin2 == 9) && (binName == bin2)
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.And(
					Exp.Or(
						Exp.And(
							Exp.GT(Exp.IntBin("bin2"), Exp.Val(40)),
							Exp.LT(Exp.IntBin("bin2"), Exp.Val(44))),
						Exp.EQ(Exp.IntBin("bin2"), Exp.Val(22)),
						Exp.EQ(Exp.IntBin("bin2"), Exp.Val(9))),
					Exp.EQ(Exp.IntBin(binName), Exp.IntBin("bin2"))))
			};

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

		[TestMethod]
		public void QueryNot()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// ! (bin2 >= 15 && bin2 <= 42)
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.Not(
					Exp.And(
						Exp.GE(Exp.IntBin("bin2"), Exp.Val(15)),
						Exp.LE(Exp.IntBin("bin2"), Exp.Val(42)))))
			};

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

		[TestMethod]
		public void QueryLastUpdate()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// record last update time > (currentTimeMillis() * 1000000L + 100)
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.GT(
					Exp.LastUpdate(),
					Exp.Val(DateTime.UtcNow.Add(TimeSpan.FromSeconds(1.0)))))
			};

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

		[TestMethod]
		public void QueryList1()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// List bin contains at least one integer item == 4
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.GT(
					ListExp.GetByValue(ListReturnType.COUNT, Exp.Val(4), Exp.ListBin("listbin")),
					Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryList2()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// List bin does not contain integer item == 5
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.EQ(
					ListExp.GetByValue(ListReturnType.COUNT, Exp.Val(5), Exp.ListBin("listbin")),
					Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryList3()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// list[4] == 20
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.EQ(
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(4), Exp.ListBin("listbin")),
					Exp.Val(20)))
			};

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

		[TestMethod]
		public void QueryMap1()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains key "B"
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.GT(
					MapExp.GetByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.Val("B"), Exp.MapBin("mapbin")),
					Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryMap2()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains value "BBB"
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				MapExp.GetByValue(MapReturnType.EXISTS, Exp.Val("BBB"), Exp.MapBin("mapbin")))
			};

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

		[TestMethod]
		public void QueryMap3()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin does not contains key "D"
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.EQ(
					MapExp.GetByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.Val("D"), Exp.MapBin("mapbin")),
					Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryMap4()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin does not contains value "AAA"
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
				Exp.EQ(
					MapExp.GetByValue(MapReturnType.COUNT, Exp.Val("AAA"), Exp.MapBin("mapbin")),
					Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryMap5()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains keys "A" and "C".
			QueryPolicy policy = new();

			List<string> list = ["A", "C"];

			policy.filterExp = Exp.Build(
				Exp.EQ(
					MapExp.Size(
						MapExp.GetByKeyList(MapReturnType.KEY_VALUE, Exp.Val(list), Exp.MapBin("mapbin"))),
					Exp.Val(2)));

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

		[TestMethod]
		public void QueryMap6()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Map bin contains keys "A" and "C".
			QueryPolicy policy = new();

			List<string> list = ["A", "C"];

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size( // return type VALUE returns a list
						MapExp.GetByKeyList(MapReturnType.VALUE, Exp.Val(list), Exp.MapBin("mapbin"))),
					Exp.Val(2)));

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

		[TestMethod]
		public void QueryDigestModulo()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Record key digest % 3 == 1
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.EQ(Exp.DigestModulo(3), Exp.Val(1)))
			};

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

		[TestMethod]
		public void QueryBinExists()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.BinExists("bin2"))
			};

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

		[TestMethod]
		public void QueryBinType()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.EQ(Exp.BinType("listbin"), Exp.Val((int)ParticleType.LIST)))
			};

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

		[TestMethod]
		public void QueryRecordSize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// This just tests that the expression was sent correctly
			// because all record sizes are effectively allowed
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.GE(Exp.RecordSize(), Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryDeviceSize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// storage-engine could be memory for which DeviceSize() returns zero.
			// This just tests that the expression was sent correctly
			// because all device sizes are effectively allowed.
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.GE(Exp.DeviceSize(), Exp.Val(0)))
			};

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

		[TestMethod]
		public void QueryMemorySize()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// storage-engine could be memory for which MemorySize() returns zero.
			// This just tests that the expression was sent correctly
			// because all memory sizes are effectively allowed.
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.GE(Exp.MemorySize(), Exp.Val(0)))
			};

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
	}
}
