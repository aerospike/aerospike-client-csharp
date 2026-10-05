/*
 * Copyright 2012-2026 Aerospike, Inc.
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
	/// <summary>
	/// End-to-end tests for Top-K queries.
	/// </summary>
	[TestClass]
	public class TestQueryReduce : TestSync
	{
		private const string IndexName = "reduceindex";
		private const string KeyPrefix = "reducekey";
		private const string BinName = "reducebin";
		private const int Size = 10;

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new() { totalTimeout = 0 };

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, IndexName, BinName, IndexType.INTEGER);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= Size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, KeyPrefix + i);
				client.Put(null, key, new Bin(BinName, i));
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, IndexName);
		}

		private static Statement BaseStatement()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(BinName);
			stmt.SetFilter(Filter.Range(BinName, 1, Size));
			return stmt;
		}

		[TestMethod]
		public void TopKDescending()
		{
			Statement stmt = BaseStatement();
			int k = 3;
			stmt.SetOrderBy(BinName, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(k);

			AssertTopK(stmt, k, new long[] { 10, 9, 8 });
		}

		[TestMethod]
		public void TopKAscending()
		{
			Statement stmt = BaseStatement();
			int k = 3;
			stmt.SetOrderBy(BinName, BinDataType.INTEGER, Order.ASC);
			stmt.SetTopK(k);

			AssertTopK(stmt, k, new long[] { 1, 2, 3 });
		}

		[TestMethod]
		public void TopKNilBinsRankLast()
		{
			string nilSet = "reduceNilSet";
			Key[] validKeys = new Key[Size];
			Key noBinKey = new(SuiteHelpers.ns, nilSet, KeyPrefix + "_nobin");
			Key wrongTypeKey = new(SuiteHelpers.ns, nilSet, KeyPrefix + "_wrongtype");
			Key collectionKey = new(SuiteHelpers.ns, nilSet, KeyPrefix + "_collection");

			try
			{
				for (int i = 1; i <= Size; i++)
				{
					validKeys[i - 1] = new Key(SuiteHelpers.ns, nilSet, KeyPrefix + i);
					client.Put(null, validKeys[i - 1], new Bin(BinName, i));
				}
				client.Put(null, noBinKey, new Bin("otherbin", 1));
				client.Put(null, wrongTypeKey, new Bin(BinName, "notanumber"));
				client.Put(null, collectionKey, new Bin(BinName, new List<object> { 1, 2, 3 }));

				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(nilSet);
				int k = Size + 3;
				stmt.SetOrderBy(BinName, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(k);

				RecordSet rs = client.Query(null, stmt);
				int count = 0;

				try
				{
					while (rs.Next())
					{
						count++;

						if (count <= Size)
						{
							Assert.AreEqual(Size - count + 1, rs.Record.GetLong(BinName));
						}
					}
				}
				finally
				{
					rs.Close();
				}
				Assert.AreEqual(Size + 3, count);
			}
			finally
			{
				foreach (Key key in validKeys)
				{
					if (key != null)
					{
						client.Delete(null, key);
					}
				}
				client.Delete(null, noBinKey);
				client.Delete(null, wrongTypeKey);
				client.Delete(null, collectionKey);
			}
		}

		[TestMethod]
		public void TopKDoubleNaN()
		{
			string nanSet = "reduceNaNSet";
			Key one = new(SuiteHelpers.ns, nanSet, KeyPrefix + "_one");
			Key two = new(SuiteHelpers.ns, nanSet, KeyPrefix + "_two");
			Key nan = new(SuiteHelpers.ns, nanSet, KeyPrefix + "_nan");

			try
			{
				client.Put(null, one, new Bin(BinName, 1.0));
				client.Put(null, two, new Bin(BinName, 2.0));
				client.Put(null, nan, new Bin(BinName, double.NaN));

				AssertDoubleTopK(TopKDoubleStatement(nanSet, Order.ASC), false);
				AssertDoubleTopK(TopKDoubleStatement(nanSet, Order.DESC), true);
			}
			finally
			{
				client.Delete(null, one);
				client.Delete(null, two);
				client.Delete(null, nan);
			}
		}

		private static void AssertTopK(Statement stmt, int k, long[] expected)
		{
			RecordSet rs = client.Query(null, stmt);
			int count = 0;

			try
			{
				while (rs.Next())
				{
					Assert.AreEqual(expected[count], rs.Record.GetLong(BinName));
					count++;
				}
			}
			finally
			{
				rs.Close();
			}

			Assert.AreEqual(k, count);
		}

		private static Statement TopKDoubleStatement(string setName, Order order)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetOrderBy(BinName, BinDataType.DOUBLE, order);
			stmt.SetTopK(2);
			return stmt;
		}

		private static void AssertDoubleTopK(Statement stmt, bool firstNaN)
		{
			using RecordSet rs = client.Query(null, stmt);
			Assert.IsTrue(rs.Next());
			Assert.AreEqual(firstNaN, double.IsNaN(rs.Record.GetDouble(BinName)));
			Assert.IsTrue(rs.Next());
			Assert.AreEqual(2.0, rs.Record.GetDouble(BinName), 0.0);
			Assert.IsFalse(rs.Next());
		}
	}
}
