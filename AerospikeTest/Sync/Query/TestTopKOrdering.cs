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
	/// Live Top-K ordering / path cases aligned with the Node.js Top-K QE plan
	/// (TK-O*, TK-P1, TK-P11) for behaviours the C# unit suite did not cover
	/// against a real 8.2.1+ cluster.
	/// </summary>
	[TestClass]
	public class TestTopKOrdering : TestSync
	{
		private const string IndexName = "topk_ord_cat_idx";
		private const string SiSet = "topKOrdSi";
		private const string ScoreBin = "score";
		private const string TsBin = "ts";
		private const string IdBin = "id";
		private const string DoubleBin = "d";
		private const string BytesBin = "b";
		private const string CatBin = "cat";
		private const string NegBin = "negM";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_2_1, "Top-K");
			RequireCapableCluster();

			Policy indexPolicy = new() { totalTimeout = 0 };

			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, SiSet, IndexName, CatBin, IndexType.STRING).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			try
			{
				client.DropIndex(null, SuiteHelpers.ns, SiSet, IndexName);
			}
			catch (AerospikeException)
			{
			}
		}

		private static void RequireCapableCluster()
		{
			Node[] nodes = client.Nodes;
			if (nodes.Length == 0)
			{
				Assert.Inconclusive("No cluster nodes");
			}

			foreach (Node node in nodes)
			{
				if (!node.HasQueryOrderBy)
				{
					Assert.Inconclusive("Cluster must advertise query-order-by");
				}
			}
		}

		/// <summary>TK-O2b: NaN ranks above +∞; missing ranks last (both directions).</summary>
		[TestMethod]
		public void DoubleNanInfAndMissingOrder()
		{
			string set = "topKOrdNan";
			Key[] keys =
			[
				Key(set, "finite"),
				Key(set, "nan"),
				Key(set, "pinf"),
				Key(set, "ninf"),
				Key(set, "missing")
			];

			client.Put(null, keys[0], new Bin(DoubleBin, 1.5));
			client.Put(null, keys[1], new Bin(DoubleBin, double.NaN));
			client.Put(null, keys[2], new Bin(DoubleBin, double.PositiveInfinity));
			client.Put(null, keys[3], new Bin(DoubleBin, double.NegativeInfinity));
			client.Put(null, keys[4], new Bin("marker", 1));

			try
			{
				List<object> desc = Ranked(set, DoubleBin, BinDataType.DOUBLE, Order.DESC, 5, DoubleBin);
				Assert.AreEqual(5, desc.Count);
				Assert.IsTrue(double.IsNaN((double)desc[0]));
				Assert.AreEqual(double.PositiveInfinity, desc[1]);
				Assert.AreEqual(1.5, desc[2]);
				Assert.AreEqual(double.NegativeInfinity, desc[3]);
				Assert.IsNull(desc[4]);

				List<object> asc = Ranked(set, DoubleBin, BinDataType.DOUBLE, Order.ASC, 5, DoubleBin);
				Assert.AreEqual(5, asc.Count);
				Assert.AreEqual(double.NegativeInfinity, asc[0]);
				Assert.AreEqual(1.5, asc[1]);
				Assert.AreEqual(double.PositiveInfinity, asc[2]);
				Assert.IsTrue(double.IsNaN((double)asc[3]));
				Assert.IsNull(asc[4]);
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>TK-O6 live: −0.0 equals 0.0 under Top-K pushdown (tie → digest).</summary>
		[TestMethod]
		public void NegativeZeroTiesWithPositiveZero()
		{
			string set = "topKOrdNeg0";
			Key[] keys =
			[
				Key(set, "neg"),
				Key(set, "pos"),
				Key(set, "one")
			];

			client.Put(null, keys[0], new Bin(DoubleBin, -0.0));
			client.Put(null, keys[1], new Bin(DoubleBin, 0.0));
			client.Put(null, keys[2], new Bin(DoubleBin, 1.0));

			try
			{
				List<object> ranked = Ranked(set, DoubleBin, BinDataType.DOUBLE, Order.ASC, 3, DoubleBin);
				Assert.AreEqual(3, ranked.Count);
				Assert.AreEqual(0.0, (double)ranked[0]);
				Assert.AreEqual(0.0, (double)ranked[1]);
				Assert.AreEqual(1.0, (double)ranked[2]);
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>TK-O4: BYTES lexicographic order; a prefix ranks before the longer value.</summary>
		[TestMethod]
		public void BytesOrderPrefixRules()
		{
			string set = "topKOrdBytes";
			(string id, byte[] value)[] rows =
			[
				("empty", []),
				("00ff", [0x00, 0xff]),
				("01", [0x01]),
				("0100", [0x01, 0x00]),
				("02", [0x02])
			];

			foreach ((string id, byte[] value) in rows)
			{
				client.Put(null, Key(set, id), new Bin(BytesBin, value));
			}

			try
			{
				List<object> asc = Ranked(set, BytesBin, BinDataType.BYTES, Order.ASC, 5, BytesBin);
				Assert.AreEqual(5, asc.Count);
				CollectionAssert.AreEqual(new byte[] { }, (byte[])asc[0]);
				CollectionAssert.AreEqual(new byte[] { 0x00, 0xff }, (byte[])asc[1]);
				CollectionAssert.AreEqual(new byte[] { 0x01 }, (byte[])asc[2]);
				CollectionAssert.AreEqual(new byte[] { 0x01, 0x00 }, (byte[])asc[3]);
				CollectionAssert.AreEqual(new byte[] { 0x02 }, (byte[])asc[4]);
			}
			finally
			{
				foreach ((string id, _) in rows)
				{
					client.Delete(null, Key(set, id));
				}
			}
		}

		/// <summary>TK-O7 / thenOrderBy: primary DESC, secondary ASC breaks ties.</summary>
		[TestMethod]
		public void ThenOrderByBreaksTiesLive()
		{
			string set = "topKOrdThen";
			(string id, long score, long ts)[] rows =
			[
				("2b", 2, 2),
				("1a", 1, 1),
				("2a", 2, 1),
				("3z", 3, 9)
			];

			foreach ((string id, long score, long ts) in rows)
			{
				client.Put(null, Key(set, id),
					new Bin(IdBin, id),
					new Bin(ScoreBin, score),
					new Bin(TsBin, ts));
			}

			try
			{
				Statement stmt = Base(set, IdBin, ScoreBin, TsBin);
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.ThenOrderBy(TsBin, BinDataType.INTEGER, Order.ASC);
				stmt.SetTopK(4);

				CollectionAssert.AreEqual(
					new[] { "3z", "2a", "2b", "1a" },
					StringValues(stmt, IdBin));
			}
			finally
			{
				foreach ((string id, _, _) in rows)
				{
					client.Delete(null, Key(set, id));
				}
			}
		}

		/// <summary>TK-O17: list and map order values rank last (wrong type), like missing.</summary>
		[TestMethod]
		public void ListAndMapOrderValuesRankLast()
		{
			string set = "topKOrdWrong";
			Key[] keys =
			[
				Key(set, "hi"),
				Key(set, "list"),
				Key(set, "map"),
				Key(set, "lo"),
				Key(set, "missing")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 3));
			client.Put(null, keys[1], new Bin(ScoreBin, new List<object> { 1, 2 }));
			client.Put(null, keys[2], new Bin(ScoreBin, new Dictionary<object, object> { { "a", 1 } }));
			client.Put(null, keys[3], new Bin(ScoreBin, 1));
			client.Put(null, keys[4], new Bin("marker", 0));

			try
			{
				foreach (Order order in new[] { Order.DESC, Order.ASC })
				{
					List<object> ranked = Ranked(set, ScoreBin, BinDataType.INTEGER, order, 5, ScoreBin);
					Assert.AreEqual(5, ranked.Count);

					if (order == Order.DESC)
					{
						Assert.AreEqual(3L, ToLong(ranked[0]));
						Assert.AreEqual(1L, ToLong(ranked[1]));
					}
					else
					{
						Assert.AreEqual(1L, ToLong(ranked[0]));
						Assert.AreEqual(3L, ToLong(ranked[1]));
					}

					// Wrong-type and missing order keys share last place (digest break).
					Assert.IsFalse(IsInteger(ranked[2]));
					Assert.IsFalse(IsInteger(ranked[3]));
					Assert.IsFalse(IsInteger(ranked[4]));
				}
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>TK-P1: secondary-index filter + Top-K.</summary>
		[TestMethod]
		public void SecondaryIndexFilterWithTopK()
		{
			Key[] keys = new Key[10];

			for (int i = 0; i < 10; i++)
			{
				keys[i] = Key(SiSet, "si_" + i);
				client.Put(null, keys[i],
					new Bin(ScoreBin, i),
					new Bin(CatBin, (i % 2 == 0) ? "even" : "odd"));
			}

			try
			{
				Statement stmt = Base(SiSet, ScoreBin);
				stmt.SetFilter(Filter.Equal(CatBin, "even"));
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(3);

				CollectionAssert.AreEqual(new long[] { 8, 6, 4 }, LongValues(stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// TK-P11 / drift #16: project only a computed order bin. RESPOND_ALL_OPS
		/// must keep incomparable records when EVAL_NO_FAIL is set.
		/// </summary>
		[TestMethod]
		public void ProjectOnlyComputedOrderKeyRespondAllOps()
		{
			string set = "topKOrdResp";
			Key[] keys =
			[
				Key(set, "1"),
				Key(set, "2"),
				Key(set, "3"),
				Key(set, "missingA"),
				Key(set, "missingB")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 1));
			client.Put(null, keys[1], new Bin(ScoreBin, 2));
			client.Put(null, keys[2], new Bin(ScoreBin, 3));
			client.Put(null, keys[3], new Bin("marker", 1));
			client.Put(null, keys[4], new Bin("marker", 2));

			try
			{
				List<object> withFail = ComputedNegScores(set, ExpReadFlags.DEFAULT, 5);
				Assert.AreEqual(3, withFail.Count);
				CollectionAssert.AreEqual(new object[] { -1L, -2L, -3L }, withFail.ToArray());

				List<object> noFail = ComputedNegScores(set, ExpReadFlags.EVAL_NO_FAIL, 5);
				Assert.AreEqual(5, noFail.Count);
				Assert.AreEqual(-1L, noFail[0]);
				Assert.AreEqual(-2L, noFail[1]);
				Assert.AreEqual(-3L, noFail[2]);
				Assert.IsNull(noFail[3]);
				Assert.IsNull(noFail[4]);
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		private static List<object> ComputedNegScores(string set, ExpReadFlags flags, int k)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(set);
			stmt.Operations =
			[
				ExpOperation.Read(NegBin,
					Exp.Build(Exp.Mul(Exp.IntBin(ScoreBin), Exp.Val(-1))),
					flags)
			];
			stmt.SetOrderBy(NegBin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(k);

			List<object> values = new();
			using RecordSet rs = client.Query(null, stmt);

			while (rs.Next())
			{
				object value = rs.Record.GetValue(NegBin);
				values.Add(IsInteger(value) ? Convert.ToInt64(value) : value);
			}

			return values;
		}

		private static List<object> Ranked(
			string set,
			string orderBin,
			BinDataType type,
			Order order,
			int k,
			string valueBin)
		{
			Statement stmt = Base(set, valueBin);
			stmt.SetOrderBy(orderBin, type, order);
			stmt.SetTopK(k);

			List<object> values = new();
			using RecordSet rs = client.Query(null, stmt);

			while (rs.Next())
			{
				object value = rs.Record.GetValue(valueBin);
				values.Add(IsInteger(value) ? Convert.ToInt64(value) : value);
			}

			return values;
		}

		private static long[] LongValues(Statement stmt, string bin)
		{
			List<long> values = new();
			using RecordSet rs = client.Query(null, stmt);

			while (rs.Next())
			{
				values.Add(rs.Record.GetLong(bin));
			}

			return values.ToArray();
		}

		private static string[] StringValues(Statement stmt, string bin)
		{
			List<string> values = new();
			using RecordSet rs = client.Query(null, stmt);

			while (rs.Next())
			{
				values.Add(rs.Record.GetString(bin));
			}

			return values.ToArray();
		}

		private static Statement Base(string set, params string[] bins)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(set);
			stmt.SetBinNames(bins);
			return stmt;
		}

		private static Key Key(string set, string userKey) => new(SuiteHelpers.ns, set, userKey);

		private static void DeleteAll(Key[] keys)
		{
			foreach (Key key in keys)
			{
				client.Delete(null, key);
			}
		}

		private static bool IsInteger(object value) => value is long or int;

		private static long ToLong(object value) => Convert.ToInt64(value);
	}
}
