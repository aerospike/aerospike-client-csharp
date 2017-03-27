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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryPredExp : TestSync
	{
		private static readonly string setName = args.set + "p";
		private const string indexName = "pred";
		private const string keyPrefix = "pred";
		private const string binName = "predint";
		private const int size = 50;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask itask = client.CreateIndex(policy, args.ns, setName, indexName, binName, IndexType.NUMERIC);
			itask.Wait();

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
				client.Put(null, key, new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map));
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, args.ns, setName, indexName);
		}

		[TestMethod]
		public void QueryPredicate1()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(40),
				PredExp.IntegerGreater(),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(44),
				PredExp.IntegerLess(),
				PredExp.And(2),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(22),
				PredExp.IntegerEqual(),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(9),
				PredExp.IntegerEqual(),
				PredExp.Or(3),
				PredExp.IntegerBin(binName),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerEqual(),
				PredExp.And(2)
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate2()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(15),
				PredExp.IntegerGreaterEq(),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(42),
				PredExp.IntegerLessEq(),
				PredExp.And(2),
				PredExp.Not()
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate3()
		{
			int begin = 10;
			int end = 45;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.RecLastUpdate(),
				PredExp.IntegerValue(DateTime.UtcNow.Add(TimeSpan.FromSeconds(1.0))),
				PredExp.IntegerGreater()
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate4()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.IntegerVar("x"),
				PredExp.IntegerValue(4),
				PredExp.IntegerEqual(),
				PredExp.ListBin("listbin"),
				PredExp.ListIterateOr("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate5()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.IntegerVar("x"),
				PredExp.IntegerValue(5),
				PredExp.IntegerUnequal(),
				PredExp.ListBin("listbin"),
				PredExp.ListIterateAnd("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate6()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.StringVar("x"),
				PredExp.StringValue("B"),
				PredExp.StringEqual(),
				PredExp.MapBin("mapbin"),
				PredExp.MapKeyIterateOr("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate7()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.StringVar("x"),
				PredExp.StringValue("BBB"),
				PredExp.StringEqual(),
				PredExp.MapBin("mapbin"),
				PredExp.MapValIterateOr("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate8()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.StringVar("x"),
				PredExp.StringValue("D"),
				PredExp.StringUnequal(),
				PredExp.MapBin("mapbin"),
				PredExp.MapKeyIterateAnd("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate9()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.StringVar("x"),
				PredExp.StringValue("AAA"),
				PredExp.StringUnequal(),
				PredExp.MapBin("mapbin"),
				PredExp.MapValIterateAnd("x")
				);

			RecordSet rs = client.Query(null, stmt);

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
		public void QueryPredicate10()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.RecDigestModulo(3),
				PredExp.IntegerValue(1),
				PredExp.IntegerEqual()
				);

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					//Console.WriteLine(rs.Record.ToString());
					count++;
				}
				Assert.AreEqual(2, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
