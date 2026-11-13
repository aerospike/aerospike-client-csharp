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
	/// Live Top-K ordering and query-path cases against a capable cluster.
	/// </summary>
	[TestClass]
	public class TestTopKOrdering : TestSync
	{
		private const string IndexName = "topk_ord_cat_idx";
		private const string ListIndexName = "topk_ord_tags_idx";
		private const string SiSet = "topKOrdSi";
		private const string ListSet = "topKOrdList";
		private const string ScoreBin = "score";
		private const string TsBin = "ts";
		private const string IdBin = "id";
		private const string DoubleBin = "d";
		private const string BytesBin = "b";
		private const string CatBin = "cat";
		private const string TagsBin = "tags";
		private const string NegBin = "negM";
		private const string VecBin = "vec";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireCapableCluster();

			Policy indexPolicy = new() { totalTimeout = 0 };

			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, SiSet, IndexName, CatBin, IndexType.STRING).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}

			try
			{
				client.CreateIndex(
					indexPolicy,
					SuiteHelpers.ns,
					ListSet,
					ListIndexName,
					TagsBin,
					IndexType.INTEGER,
					IndexCollectionType.LIST).Wait();
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

			try
			{
				client.DropIndex(null, SuiteHelpers.ns, ListSet, ListIndexName);
			}
			catch (AerospikeException)
			{
			}
		}

		private static void RequireCapableCluster() => RequireQueryOrderBySupport();

		/// <summary>NaN ranks above +∞; missing ranks last (both directions).</summary>
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

		/// <summary>−0.0 equals 0.0 under Top-K pushdown (tie → digest).</summary>
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

		/// <summary>BYTES lexicographic order; a prefix ranks before the longer value.</summary>
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

		/// <summary>Primary DESC with secondary ASC breaks ties.</summary>
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

		/// <summary>List and map order values rank last (wrong type), like missing.</summary>
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

		/// <summary>A vector particle in a scalar order bin ranks last (wrong type).</summary>
		[TestMethod]
		public void VectorOrderValueRanksLast()
		{
			RequireVectorSupport();

			string set = "topKOrdVec";
			Key[] keys =
			[
				Key(set, "hi"),
				Key(set, "vec"),
				Key(set, "lo")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 3));
			client.Put(null, keys[1], new Bin(ScoreBin, Vector.OfFloat32([1f, 2f, 3f])));
			client.Put(null, keys[2], new Bin(ScoreBin, 1));

			try
			{
				List<object> ranked = Ranked(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 3, ScoreBin);
				Assert.AreEqual(3, ranked.Count);
				Assert.AreEqual(3L, ToLong(ranked[0]));
				Assert.AreEqual(1L, ToLong(ranked[1]));
				Assert.IsTrue(ranked[2] is Vector, "vector order value should rank last");
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AO-07: HLL order values rank last like vector / wrong-type bins.</summary>
		[TestMethod]
		public void HllOrderValueRanksLast()
		{
			string set = "topKOrdHll";
			Key[] keys =
			[
				Key(set, "hi"),
				Key(set, "hll"),
				Key(set, "lo")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 3));
			client.Operate(null, keys[1], HLLOperation.Init(HLLPolicy.Default, ScoreBin, 8));
			client.Put(null, keys[2], new Bin(ScoreBin, 1));

			try
			{
				List<object> ranked = Ranked(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 3, ScoreBin);
				Assert.AreEqual(3, ranked.Count);
				Assert.AreEqual(3L, ToLong(ranked[0]));
				Assert.AreEqual(1L, ToLong(ranked[1]));
				Assert.IsFalse(IsInteger(ranked[2]), "HLL order value should rank last");
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-04: live Top-K with k = 1 and k = 1,000.</summary>
		[TestMethod]
		public void TopKExactForKOneAndThousand()
		{
			string set = "topKOrdKBound";
			const int n = 1000;
			Key[] keys = new Key[n];

			for (int i = 0; i < n; i++)
			{
				keys[i] = Key(set, "k" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				Statement k1 = Base(set, ScoreBin);
				k1.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				k1.SetTopK(1);
				CollectionAssert.AreEqual(new long[] { 999 }, LongValues(k1, ScoreBin));

				Statement k1000 = Base(set, ScoreBin);
				k1000.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				k1000.SetTopK(1000);
				long[] topThousand = LongValues(k1000, ScoreBin);
				Assert.AreEqual(1000, topThousand.Length);
				Assert.AreEqual(999, topThousand[0]);
				Assert.AreEqual(0, topThousand[999]);
				for (int i = 1; i < topThousand.Length; i++)
				{
					Assert.IsTrue(topThousand[i - 1] > topThousand[i]);
				}
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AT-11: bin names + operations together — ops win; no Top-K-specific rejection.</summary>
		[TestMethod]
		public void TopKWithBinsAndOperationsUsesOperations()
		{
			string set = "topKOrdBinsOps";
			Key[] keys =
			[
				Key(set, "a"),
				Key(set, "b"),
				Key(set, "c")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 1), new Bin(IdBin, "a"));
			client.Put(null, keys[1], new Bin(ScoreBin, 3), new Bin(IdBin, "b"));
			client.Put(null, keys[2], new Bin(ScoreBin, 2), new Bin(IdBin, "c"));

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(set);
				stmt.SetBinNames(IdBin); // ignored when Operations is set
				stmt.Operations =
				[
					Operation.Get(ScoreBin),
					ExpOperation.Read(NegBin, Exp.Build(Exp.Mul(Exp.IntBin(ScoreBin), Exp.Val(-1))), ExpReadFlags.DEFAULT)
				];
				stmt.SetOrderBy(NegBin, BinDataType.INTEGER, Order.ASC);
				stmt.SetTopK(2);

				CollectionAssert.AreEqual(new long[] { 3, 2 }, LongValues(stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AT-12: short-query hint + socketTimeout 0 + totalTimeout still runs as long Top-K.</summary>
		[TestMethod]
		public void TopKAcceptsShortQueryHintAndTimeouts()
		{
			string set = "topKOrdShort";
			Key[] keys =
			[
				Key(set, "1"),
				Key(set, "2"),
				Key(set, "3")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 10));
			client.Put(null, keys[1], new Bin(ScoreBin, 30));
			client.Put(null, keys[2], new Bin(ScoreBin, 20));

			try
			{
				Statement stmt = Base(set, ScoreBin);
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(2);

				QueryPolicy policy = new()
				{
#pragma warning disable 0618
					shortQuery = true,
#pragma warning restore 0618
					socketTimeout = 0,
					totalTimeout = 10000
				};

				CollectionAssert.AreEqual(new long[] { 30, 20 }, LongValues(policy, stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-08: every replica policy still yields the exact global Top-K (query ignores replica).</summary>
		[TestMethod]
		public void TopKExactUnderEveryReplicaPolicy()
		{
			string set = "topKOrdReplica";
			Key[] keys = new Key[20];
			for (int i = 0; i < keys.Length; i++)
			{
				keys[i] = Key(set, "r" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				Statement stmt = Base(set, ScoreBin);
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(5);
				long[] expected = [19, 18, 17, 16, 15];

				foreach (Replica replica in Enum.GetValues(typeof(Replica)))
				{
					QueryPolicy policy = new() { replica = replica };
					try
					{
						CollectionAssert.AreEqual(expected, LongValues(policy, stmt, ScoreBin),
							"replica=" + replica);
					}
					catch (AerospikeException ae) when (ae.Result == ResultCode.PARAMETER_ERROR &&
						ae.Message.Contains("Invalid replica"))
					{
						// Documented client refusal for query-incompatible replica values (e.g. RANDOM).
						Assert.AreEqual(Replica.RANDOM, replica);
					}
				}
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-01: Top-K with expression secondary index filter.</summary>
		[TestMethod]
		public void ExpressionIndexFilterWithTopK()
		{
			const string set = "topKOrdExpIdx";
			const string indexName = "topk_ord_exp_idx";
			Policy indexPolicy = new() { totalTimeout = 0 };
			Expression exp = Exp.Build(Exp.IntBin(ScoreBin));

			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, set, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, exp).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}

			Key[] keys = new Key[10];
			for (int i = 0; i < 10; i++)
			{
				keys[i] = Key(set, "e" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				Statement stmt = Base(set, ScoreBin);
				stmt.SetFilter(Filter.Range(exp, 5, 9));
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(3);

				CollectionAssert.AreEqual(new long[] { 9, 8, 7 }, LongValues(stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
				try
				{
					client.DropIndex(null, SuiteHelpers.ns, set, indexName);
				}
				catch (AerospikeException)
				{
				}
			}
		}

		/// <summary>AO-01: INTEGER order with int64 extremes.</summary>
		[TestMethod]
		public void IntegerExtremesOrderExact()
		{
			string set = "topKOrdI64";
			Key[] keys =
			[
				Key(set, "min"),
				Key(set, "neg"),
				Key(set, "zero"),
				Key(set, "pos"),
				Key(set, "max")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, long.MinValue));
			client.Put(null, keys[1], new Bin(ScoreBin, -1L));
			client.Put(null, keys[2], new Bin(ScoreBin, 0L));
			client.Put(null, keys[3], new Bin(ScoreBin, 1L));
			client.Put(null, keys[4], new Bin(ScoreBin, long.MaxValue));

			try
			{
				CollectionAssert.AreEqual(
					new long[] { long.MaxValue, 1L, 0L, -1L, long.MinValue },
					LongValues(Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 5), ScoreBin));
				CollectionAssert.AreEqual(
					new long[] { long.MinValue, -1L, 0L, 1L, long.MaxValue },
					LongValues(Ordered(set, ScoreBin, BinDataType.INTEGER, Order.ASC, 5), ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AO-11: seeded random Top-K combinations match an independent reference sort.</summary>
		[TestMethod]
		public void SeededCombinatorialMatchesReference()
		{
			const int seed = 20261113;
			Random rng = new(seed);
			string set = "topKOrdFuzz";
			const int n = 40;
			Key[] keys = new Key[n];
			long[] scores = new long[n];

			// Unique scores so the independent sort matches Top-K without digest ties.
			for (int i = 0; i < n; i++)
			{
				scores[i] = i;
			}
			for (int i = n - 1; i > 0; i--)
			{
				int j = rng.Next(i + 1);
				(scores[i], scores[j]) = (scores[j], scores[i]);
			}
			for (int i = 0; i < n; i++)
			{
				keys[i] = Key(set, "f" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, scores[i]), new Bin(IdBin, i));
			}

			try
			{
				for (int trial = 0; trial < 8; trial++)
				{
					int k = rng.Next(1, 11);
					Order order = rng.Next(2) == 0 ? Order.DESC : Order.ASC;
					long[] actual = LongValues(Ordered(set, ScoreBin, BinDataType.INTEGER, order, k), ScoreBin);
					long[] expected = (long[])scores.Clone();
					Array.Sort(expected);
					if (order == Order.DESC)
					{
						Array.Reverse(expected);
					}
					long[] top = new long[k];
					Array.Copy(expected, top, k);
					CollectionAssert.AreEqual(top, actual, "seed=" + seed + " trial=" + trial);
				}
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AO-12: projecting the vector bin does not change distance Top-K order.</summary>
		[TestMethod]
		public void DistanceOrderStableWithOrWithoutVectorProjection()
		{
			RequireVectorSupport();

			string set = "topKOrdDistProj";
			Vector query = Vector.OfFloat32([0f, 0f]);
			Key[] keys = new Key[5];
			for (int i = 0; i < 5; i++)
			{
				keys[i] = Key(set, "d" + i);
				client.Put(null, keys[i],
					new Bin(IdBin, i),
					new Bin(ScoreBin, i),
					new Bin(VecBin, Vector.OfFloat32([i, 0f])));
			}

			try
			{
				Expression dist = Exp.Build(VectorExp.Distance(
					VectorDistanceMetric.EUCLIDEAN_SQUARED, query, Exp.VectorBin(VecBin)));

				long[] withoutVec = DistanceTopKIds(set, dist, includeVector: false, k: 3);
				long[] withVec = DistanceTopKIds(set, dist, includeVector: true, k: 3);
				CollectionAssert.AreEqual(withoutVec, withVec);

				// Order by ScoreBin while also returning distance — order follows ScoreBin.
				Statement byScore = new();
				byScore.SetNamespace(SuiteHelpers.ns);
				byScore.SetSetName(set);
				byScore.Operations =
				[
					Operation.Get(ScoreBin),
					Operation.Get(IdBin),
					ExpOperation.Read(NegBin, dist, ExpReadFlags.DEFAULT)
				];
				byScore.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				byScore.SetTopK(3);
				CollectionAssert.AreEqual(new long[] { 4, 3, 2 }, LongValues(byScore, IdBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-05: Top-K on a partition range ranks only within that scope.</summary>
		[TestMethod]
		public void TopKWithinPartitionRange()
		{
			string set = "topKOrdPart";
			Key[] keys = new Key[30];
			for (int i = 0; i < 30; i++)
			{
				keys[i] = Key(set, "p" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				Statement stmt = Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 5);
				PartitionFilter range = PartitionFilter.Range(0, 1024);
				List<long> scoped = new();
				using (RecordSet rs = client.QueryPartitions(null, stmt, range))
				{
					while (rs.Next())
					{
						scoped.Add(rs.Record.GetLong(ScoreBin));
					}
				}
				Assert.IsTrue(scoped.Count <= 5);
				Assert.IsTrue(scoped.Count >= 1);
				for (int i = 1; i < scoped.Count; i++)
				{
					Assert.IsTrue(scoped[i - 1] >= scoped[i]);
				}

				HashSet<long> all = new(LongValues(Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 30), ScoreBin));
				foreach (long score in scoped)
				{
					Assert.IsTrue(all.Contains(score));
				}
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-06: Query and QueryPartitions(All) return the same Top-K.</summary>
		[TestMethod]
		public void QueryApisReturnIdenticalTopK()
		{
			string set = "topKOrdApi";
			Key[] keys = new Key[15];
			for (int i = 0; i < 15; i++)
			{
				keys[i] = Key(set, "a" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i * 10));
			}

			try
			{
				Statement stmt = Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 5);
				long[] viaQuery = LongValues(stmt, ScoreBin);

				List<long> viaParts = new();
				using (RecordSet rs = client.QueryPartitions(null, stmt, PartitionFilter.All()))
				{
					while (rs.Next())
					{
						viaParts.Add(rs.Record.GetLong(ScoreBin));
					}
				}
				CollectionAssert.AreEqual(viaQuery, viaParts.ToArray());
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-07: closing Top-K streams early does not hang or leak; next query is exact.</summary>
		[TestMethod]
		public void EarlyStreamCloseDoesNotBreakNextQuery()
		{
			string set = "topKOrdClose";
			Key[] keys = new Key[20];
			for (int i = 0; i < 20; i++)
			{
				keys[i] = Key(set, "c" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				for (int i = 0; i < 20; i++)
				{
					Statement stmt = Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 10);
					RecordSet rs = client.Query(null, stmt);
					Assert.IsTrue(rs.Next());
					rs.Close();
				}

				CollectionAssert.AreEqual(
					new long[] { 19, 18, 17 },
					LongValues(Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 3), ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AP-09: Top-K results expose bins/digest/generation and are never padded past k.</summary>
		[TestMethod]
		public void TopKResultsHaveDigestGenerationAndExactCount()
		{
			string set = "topKOrdMeta";
			Key[] keys = new Key[8];
			for (int i = 0; i < 8; i++)
			{
				keys[i] = Key(set, "m" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}

			try
			{
				Statement stmt = Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 3);
				int count = 0;
				using RecordSet rs = client.Query(null, stmt);
				while (rs.Next())
				{
					count++;
					Assert.IsNotNull(rs.Key);
					Assert.IsNotNull(rs.Key.digest);
					Assert.AreEqual(20, rs.Key.digest.Length);
					Assert.IsTrue(rs.Record.generation >= 1);
					Assert.IsNotNull(rs.Record.GetValue(ScoreBin));
				}
				Assert.AreEqual(3, count);

				Statement fewer = Ordered(set, ScoreBin, BinDataType.INTEGER, Order.DESC, 100);
				Assert.AreEqual(8, LongValues(fewer, ScoreBin).Length);
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>AM-01: multi-node global merge (inconclusive below 3 nodes).</summary>
		[TestMethod]
		public void MultiNodeGlobalTopKMerge()
		{
			if (client.Nodes.Length < 3)
			{
				Assert.Inconclusive("AM-01 requires a 3-node cluster");
			}

			string set = "topKOrdMerge3";
			const int n = 300;
			Key[] keys = new Key[n];
			for (int i = 0; i < n; i++)
			{
				keys[i] = Key(set, "m" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i), new Bin(DoubleBin, i + 0.5));
			}

			try
			{
				Statement stmt = Base(set, ScoreBin);
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(10);
				long[] top = LongValues(stmt, ScoreBin);
				Assert.AreEqual(10, top.Length);
				for (int i = 0; i < 10; i++)
				{
					Assert.AreEqual(n - 1 - i, top[i]);
				}

				// Ties across digests: same DOUBLE score, k below record count.
				for (int i = 0; i < 6; i++)
				{
					client.Put(null, keys[i], new Bin(ScoreBin, 1000), new Bin(DoubleBin, 7.0));
				}

				Statement tied = Base(set, ScoreBin, DoubleBin);
				tied.SetOrderBy(DoubleBin, BinDataType.DOUBLE, Order.DESC);
				tied.ThenOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				tied.SetTopK(4);
				Assert.AreEqual(4, LongValues(tied, ScoreBin).Length);
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// List secondary index: a record that matches several list values still appears once.
		/// </summary>
		[TestMethod]
		public void ListIndexMultiMatchReturnsRecordOnce()
		{
			Key[] keys =
			[
				Key(ListSet, "s1"),
				Key(ListSet, "s2"),
				Key(ListSet, "s3")
			];

			client.Put(null, keys[0],
				new Bin(ScoreBin, 1),
				new Bin(TagsBin, new List<object> { 5L, 5L, 7L }));
			client.Put(null, keys[1],
				new Bin(ScoreBin, 2),
				new Bin(TagsBin, new List<object> { 5L, 6L }));
			client.Put(null, keys[2],
				new Bin(ScoreBin, 3),
				new Bin(TagsBin, new List<object> { 9L }));

			try
			{
				Statement stmt = Base(ListSet, ScoreBin, TagsBin);
				stmt.SetFilter(Filter.Range(TagsBin, IndexCollectionType.LIST, 5, 7));
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(10);

				CollectionAssert.AreEqual(new long[] { 2, 1 }, LongValues(stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>Whole-namespace Top-K (no set) with a unique filter expression.</summary>
		[TestMethod]
		public void WholeNamespaceTopKWithFilterExpression()
		{
			const string markerBin = "topKNsMark";
			const long marker = 424242;
			string setA = "topKOrdNsA";
			string setB = "topKOrdNsB";

			Key[] keys =
			[
				Key(setA, "a1"),
				Key(setA, "a2"),
				Key(setB, "b1"),
				Key(setB, "b2")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 10), new Bin(markerBin, marker));
			client.Put(null, keys[1], new Bin(ScoreBin, 40), new Bin(markerBin, marker));
			client.Put(null, keys[2], new Bin(ScoreBin, 30), new Bin(markerBin, marker));
			client.Put(null, keys[3], new Bin(ScoreBin, 20), new Bin(markerBin, marker));

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetBinNames(ScoreBin, markerBin);
				stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(3);

				QueryPolicy policy = new()
				{
					filterExp = Exp.Build(Exp.EQ(Exp.IntBin(markerBin), Exp.Val(marker)))
				};

				CollectionAssert.AreEqual(new long[] { 40, 30, 20 }, LongValues(policy, stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>Secondary-index filter combined with Top-K.</summary>
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
		/// AO-10: order by a non-distance computed value (abs(n-7)), then by n.
		/// </summary>
		[TestMethod]
		public void ComputedNonDistanceOrderThenSecondaryKey()
		{
			string set = "topKOrdAbs";
			const string AbsBin = "abs7";
			Key[] keys = new Key[5];
			// n = 5,6,7,8,9 → |n-7| = 2,1,0,1,2. ASC abs then DESC n → 7, 8, 6, 9, 5
			int[] ns = [5, 6, 7, 8, 9];
			for (int i = 0; i < ns.Length; i++)
			{
				keys[i] = Key(set, "n" + ns[i]);
				client.Put(null, keys[i], new Bin(ScoreBin, ns[i]));
			}

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(set);
				stmt.Operations =
				[
					ExpOperation.Read(AbsBin,
						Exp.Build(Exp.Abs(Exp.Sub(Exp.IntBin(ScoreBin), Exp.Val(7)))),
						ExpReadFlags.DEFAULT),
					Operation.Get(ScoreBin)
				];
				stmt.SetOrderBy(AbsBin, BinDataType.INTEGER, Order.ASC);
				stmt.ThenOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(5);

				CollectionAssert.AreEqual(new long[] { 7, 8, 6, 9, 5 }, LongValues(stmt, ScoreBin));
			}
			finally
			{
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// AE-04: Top-K fails when a non-order-key projection op fails on a record
		/// (list get on an integer bin). Plain query leaves the record out. Waits on SERVER-1560.
		/// </summary>
		[TestMethod]
		public void TopKFailsWhenProjectionOpFails()
		{
			string set = "topKOrdAe04";
			Key good = Key(set, "good");
			Key bad = Key(set, "bad");
			client.Put(null, good, new Bin(ScoreBin, 10), new Bin(TagsBin, new List<object> { 1, 2 }));
			client.Put(null, bad, new Bin(ScoreBin, 20)); // integer where list op is projected

			try
			{
				Statement topK = new();
				topK.SetNamespace(SuiteHelpers.ns);
				topK.SetSetName(set);
				topK.Operations =
				[
					ListOperation.GetByIndex(TagsBin, 0, ListReturnType.VALUE),
					Operation.Get(ScoreBin)
				];
				topK.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				topK.SetTopK(2);

				try
				{
					using RecordSet rs = client.Query(null, topK);
					while (rs.Next())
					{
					}
					Assert.Inconclusive("AE-04 expects Top-K to fail on projection op error (SERVER-1560)");
				}
				catch (AerospikeException)
				{
					// Expected once SERVER-1560 ships: abandon Top-K on unreadable projection.
				}

				Statement plain = new();
				plain.SetNamespace(SuiteHelpers.ns);
				plain.SetSetName(set);
				plain.Operations =
				[
					ListOperation.GetByIndex(TagsBin, 0, ListReturnType.VALUE),
					Operation.Get(ScoreBin)
				];
				int plainCount = 0;
				using (RecordSet rs = client.Query(null, plain))
				{
					while (rs.Next())
					{
						plainCount++;
					}
				}
				Assert.IsTrue(plainCount >= 1, "plain query should leave the failing record out and continue");
			}
			finally
			{
				client.Delete(null, good);
				client.Delete(null, bad);
			}
		}

		/// <summary>
		/// AE-06: a read expression the server cannot build should fail at query start (SERVER-1560).
		/// </summary>
		[TestMethod]
		public void UnbuildableReadExpressionFailsAtQueryStart()
		{
			string set = "topKOrdAe06";
			Key key = Key(set, "ae06");
			client.Put(null, key, new Bin(ScoreBin, 1));

			try
			{
				// List get against an integer bin — server cannot build / apply the expression.
				Expression bad = Exp.Build(
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(0), Exp.ListBin(ScoreBin)));
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(set);
				stmt.Operations = [ExpOperation.Read("x", bad, ExpReadFlags.DEFAULT)];

				try
				{
					using RecordSet rs = client.Query(null, stmt);
					while (rs.Next())
					{
					}
					Assert.Inconclusive("AE-06 expects PARAMETER_ERROR at query start (SERVER-1560)");
				}
				catch (AerospikeException ae)
				{
					Assert.IsTrue(
						ae.Result == ResultCode.PARAMETER_ERROR ||
						ae.Result == ResultCode.OP_NOT_APPLICABLE ||
						ae.Result == ResultCode.BIN_TYPE_ERROR,
						"unexpected rc " + ae.Result);
				}
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>
		/// Project only a computed order bin. RESPOND_ALL_OPS must keep
		/// incomparable records when EVAL_NO_FAIL is set.
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

		private static long[] LongValues(Statement stmt, string bin) =>
			LongValues(null, stmt, bin);

		private static long[] LongValues(QueryPolicy policy, Statement stmt, string bin)
		{
			List<long> values = new();
			using RecordSet rs = client.Query(policy, stmt);

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

		private static Statement Ordered(string set, string bin, BinDataType type, Order order, int k)
		{
			Statement stmt = Base(set, bin);
			stmt.SetOrderBy(bin, type, order);
			stmt.SetTopK(k);
			return stmt;
		}

		private static long[] DistanceTopKIds(string set, Expression dist, bool includeVector, int k)
		{
			List<Operation> ops = new()
			{
				ExpOperation.Read(NegBin, dist, ExpReadFlags.DEFAULT),
				Operation.Get(IdBin)
			};
			if (includeVector)
			{
				ops.Add(Operation.Get(VecBin));
			}

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(set);
			stmt.Operations = ops.ToArray();
			stmt.SetOrderBy(NegBin, BinDataType.DOUBLE, Order.ASC);
			stmt.SetTopK(k);
			return LongValues(stmt, IdBin);
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
