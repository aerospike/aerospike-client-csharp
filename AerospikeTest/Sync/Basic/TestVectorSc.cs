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
	/// AW-11 / AW-13 / AW-14 / AP-13: vector + Top-K behaviour on an SC namespace.
	/// </summary>
	[TestClass]
	public class TestVectorSc : TestSync
	{
		private const string Set = "vecScSet";
		private const string VecBin = "embedding";
		private const string ScoreBin = "score";
		private const string IdBin = "id";
		private const string DistBin = "dist";

		[TestInitialize]
		public void RequireSc()
		{
			if (!SuiteHelpers.scMode)
			{
				Assert.Inconclusive("Strong consistency mode is required");
			}
		}

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();
		}

		/// <summary>
		/// AW-11: txn put / operate / batch write of vectors; in-txn distance read/filter;
		/// nested vector rejected; conflict + abort restore committed state.
		/// </summary>
		[TestMethod]
		public void TransactionVectorWriteReadAndConflict()
		{
			Key putKey = Key("aw11put");
			Key opKey = Key("aw11op");
			Key batchKey = Key("aw11batch");
			Key conflictKey = Key("aw11conflict");
			Key listKey = Key("aw11list");

			foreach (Key key in new[] { putKey, opKey, batchKey, conflictKey, listKey })
			{
				client.Delete(null, key);
			}

			Vector v1 = Vector.OfFloat32([1f, 0f, 0f]);
			Vector v2 = Vector.OfFloat32([0f, 1f, 0f]);
			Vector v3 = Vector.OfFloat32([0f, 0f, 1f]);

			client.Put(null, putKey, new Bin(VecBin, v1), new Bin(IdBin, 1));
			client.Put(null, conflictKey, new Bin(VecBin, v1), new Bin(IdBin, 9));

			using Txn txn = new();
			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			Policy rp = client.ReadPolicyDefault.Clone();
			rp.Txn = txn;

			client.Put(wp, putKey, new Bin(VecBin, v2));
			client.Operate(wp, opKey, Operation.Put(new Bin(VecBin, v3)), Operation.Put(new Bin(IdBin, 2)));

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Txn = txn;
			BatchResults batch = client.Operate(bp, null, [batchKey], Operation.Put(new Bin(VecBin, v2)));
			Assert.IsTrue(batch.status);

			// In-transaction read + distance expression see the txn's writes.
			Assert.AreEqual(v2, client.Get(rp, putKey).GetVector(VecBin));
			Record distRec = client.Operate(wp, putKey,
				ExpOperation.Read(DistBin,
					Exp.Build(VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, v2, Exp.VectorBin(VecBin))),
					ExpReadFlags.DEFAULT));
			Assert.AreEqual(1.0, distRec.GetDouble(DistBin), 0.001);

			Policy filterRead = client.ReadPolicyDefault.Clone();
			filterRead.Txn = txn;
			filterRead.filterExp = Exp.Build(Exp.GT(
				VectorExp.Distance(VectorDistanceMetric.COSINE_SIMILARITY, v2, Exp.VectorBin(VecBin)),
				Exp.Val(0.99)));
			Assert.IsNotNull(client.Get(filterRead, putKey));

			// Nested vector in a list is refused before send.
			AerospikeException nested = Assert.Throws<AerospikeException>(
				() => client.Put(wp, listKey, new Bin("list", new List<object> { v1 })));
			Assert.AreEqual(ResultCode.PARSE_ERROR, nested.Result);

			// Outside writer blocked while txn holds the key.
			try
			{
				client.Put(null, putKey, new Bin(VecBin, v1));
				Assert.Fail("expected MRT_BLOCKED");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.MRT_BLOCKED, ae.Result);
			}

			client.Commit(txn);

			Assert.AreEqual(v2, client.Get(null, putKey).GetVector(VecBin));
			Assert.AreEqual(v3, client.Get(null, opKey).GetVector(VecBin));
			Assert.AreEqual(v2, client.Get(null, batchKey).GetVector(VecBin));

			// Abort path restores the prior committed vector.
			using Txn abortTxn = new();
			WritePolicy awp = client.WritePolicyDefault.Clone();
			awp.Txn = abortTxn;
			client.Put(awp, conflictKey, new Bin(VecBin, v3));
			client.Abort(abortTxn);
			Assert.AreEqual(v1, client.Get(null, conflictKey).GetVector(VecBin));

			foreach (Key key in new[] { putKey, opKey, batchKey, conflictKey, listKey })
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-13: linearizable reads return the last acknowledged vector.</summary>
		[TestMethod]
		public void LinearizableReadReturnsLastAcknowledgedVector()
		{
			Key key = Key("aw13");
			client.Delete(null, key);

			Vector first = Vector.OfFloat32([1f, 2f, 3f]);
			Vector second = Vector.OfFloat32([4f, 5f, 6f]);
			client.Put(null, key, new Bin(VecBin, first));
			client.Put(null, key, new Bin(VecBin, second));

			try
			{
				Policy linearize = client.ReadPolicyDefault.Clone();
				linearize.readModeSC = ReadModeSC.LINEARIZE;
				Assert.AreEqual(second, client.Get(linearize, key).GetVector(VecBin));

				Record[] batch = client.Get(new BatchPolicy { readModeSC = ReadModeSC.LINEARIZE }, [key]);
				Assert.AreEqual(second, batch[0].GetVector(VecBin));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>
		/// AW-14: QueryPolicy.Txn is ignored — Top-K ranks the committed vector, never the
		/// uncommitted replacement; after commit the next query ranks the new vector.
		/// </summary>
		[TestMethod]
		public void TopKIgnoresUncommittedTxnVector()
		{
			RequireCapableTopK();

			const string marker = "aw14mark";
			const long mark = 141414;
			Key[] keys =
			[
				Key("aw14a"),
				Key("aw14b"),
				Key("aw14c")
			];

			client.Put(null, keys[0], new Bin(ScoreBin, 10), new Bin(marker, mark), new Bin(VecBin, Vector.OfFloat32([1f, 0f])));
			client.Put(null, keys[1], new Bin(ScoreBin, 20), new Bin(marker, mark), new Bin(VecBin, Vector.OfFloat32([0f, 1f])));
			client.Put(null, keys[2], new Bin(ScoreBin, 30), new Bin(marker, mark), new Bin(VecBin, Vector.OfFloat32([0f, 0f])));

			using Txn txn = new();
			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			// Uncommitted: push aw14a to the top score.
			client.Put(wp, keys[0], new Bin(ScoreBin, 100), new Bin(marker, mark), new Bin(VecBin, Vector.OfFloat32([9f, 9f])));

			try
			{
				Statement stmt = MarkerTopK(k: 2);
				Expression markFilter = Exp.Build(Exp.EQ(Exp.IntBin(marker), Exp.Val(mark)));
				QueryPolicy withTxn = new()
				{
					Txn = txn,
					filterExp = markFilter
				};

				// Documented: QueryPolicy.Txn is ignored — still sees committed scores.
				CollectionAssert.AreEqual(new long[] { 30, 20 }, LongScores(withTxn, stmt));

				client.Commit(txn);
				CollectionAssert.AreEqual(new long[] { 100, 30 }, LongScores(new QueryPolicy { filterExp = markFilter }, stmt));
			}
			finally
			{
				try { client.Abort(txn); } catch (AerospikeException) { }
				foreach (Key key in keys)
				{
					client.Delete(null, key);
				}
			}
		}

		/// <summary>AP-13: SC Top-K / KNN paths match the AP-01 style exact ranking.</summary>
		[TestMethod]
		public void ScTopKAndKnnMatchExactRanking()
		{
			RequireCapableTopK();

			const string set = "vecScAp13";
			Key[] keys = new Key[10];
			for (int i = 0; i < 10; i++)
			{
				keys[i] = new Key(SuiteHelpers.ns, set, "ap13_" + i);
				client.Put(null, keys[i],
					new Bin(ScoreBin, i),
					new Bin(IdBin, i),
					new Bin(VecBin, Vector.OfFloat32([i, 0f, 0f])));
			}

			try
			{
				Statement topK = new();
				topK.SetNamespace(SuiteHelpers.ns);
				topK.SetSetName(set);
				topK.SetBinNames(ScoreBin);
				topK.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
				topK.SetTopK(3);

				QueryPolicy qp = new()
				{
					filterExp = Exp.Build(Exp.And(
						Exp.GE(Exp.IntBin(ScoreBin), Exp.Val(5)),
						Exp.LE(Exp.IntBin(ScoreBin), Exp.Val(9)))),
					readModeSC = ReadModeSC.LINEARIZE
				};
				CollectionAssert.AreEqual(new long[] { 9, 8, 7 }, LongScores(qp, topK));

				Vector query = Vector.OfFloat32([0f, 0f, 0f]);
				Statement knn = new();
				knn.SetNamespace(SuiteHelpers.ns);
				knn.SetSetName(set);
				knn.Operations =
				[
					Operation.Get(IdBin),
					ExpOperation.Read(DistBin,
						Exp.Build(VectorExp.Distance(VectorDistanceMetric.EUCLIDEAN_SQUARED, query, Exp.VectorBin(VecBin))),
						ExpReadFlags.DEFAULT)
				];
				knn.SetOrderBy(DistBin, BinDataType.DOUBLE, Order.ASC);
				knn.SetTopK(3);

				QueryPolicy knnPolicy = new() { readModeSC = ReadModeSC.LINEARIZE };
				long[] ids = new long[3];
				int n = 0;
				using RecordSet rs = client.Query(knnPolicy, knn);
				while (rs.Next())
				{
					ids[n++] = rs.Record.GetLong(IdBin);
				}
				Assert.AreEqual(3, n);
				CollectionAssert.AreEqual(new long[] { 0, 1, 2 }, ids);
			}
			finally
			{
				foreach (Key key in keys)
				{
					client.Delete(null, key);
				}
			}
		}

		private static void RequireCapableTopK() => RequireQueryOrderBySupport();

		private static Statement MarkerTopK(int k)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetBinNames(ScoreBin);
			stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(k);
			return stmt;
		}

		private static long[] LongScores(QueryPolicy policy, Statement stmt)
		{
			List<long> values = new();
			using RecordSet rs = client.Query(policy, stmt);
			while (rs.Next())
			{
				values.Add(rs.Record.GetLong(ScoreBin));
			}
			return values.ToArray();
		}

		private static Key Key(string userKey) => new(SuiteHelpers.ns, Set, userKey);
	}
}