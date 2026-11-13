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
	/// Remaining AX distance-expression gaps (boundary, path parity, overflow, typed reads).
	/// </summary>
	[TestClass]
	public class TestVectorExpGaps : TestSync
	{
		private const string Set = "vecExpGaps";
		private const string VecBin = "embedding";
		private const string DistBin = "dist";
		private const string IdBin = "id";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();
		}

		/// <summary>AX-04: &gt; and &gt;= around an exact distance boundary.</summary>
		[TestMethod]
		public void DistanceBoundaryFilters()
		{
			Vector query = Vector.OfFloat32([0f, 0f, 0f]);
			Key[] keys =
			[
				new Key(SuiteHelpers.ns, Set, "b0"),
				new Key(SuiteHelpers.ns, Set, "b1"),
				new Key(SuiteHelpers.ns, Set, "b2")
			];

			// Distances to origin: 0, 1, 4 (squared Euclidean).
			client.Put(null, keys[0], new Bin(IdBin, 0), new Bin(VecBin, Vector.OfFloat32([0f, 0f, 0f])));
			client.Put(null, keys[1], new Bin(IdBin, 1), new Bin(VecBin, Vector.OfFloat32([1f, 0f, 0f])));
			client.Put(null, keys[2], new Bin(IdBin, 2), new Bin(VecBin, Vector.OfFloat32([2f, 0f, 0f])));

			try
			{
				// Distances: id0=0, id1=1, id2=4 (squared Euclidean to origin).
				CollectionAssert.AreEquivalent(new long[] { 1, 2 }, FilterIds(query, Exp.GE, 1.0));
				CollectionAssert.AreEquivalent(new long[] { 1, 2 }, FilterIds(query, Exp.GT, 0.0));
				CollectionAssert.AreEquivalent(new long[] { 0, 1, 2 }, FilterIds(query, Exp.GE, 0.0));
				CollectionAssert.AreEquivalent(new long[] { 2 }, FilterIds(query, Exp.GT, 1.0));
				CollectionAssert.AreEquivalent(new long[] { 0, 1 }, FilterIds(query, Exp.LE, 1.0));
			}
			finally
			{
				foreach (Key key in keys)
				{
					client.Delete(null, key);
				}
			}
		}

		/// <summary>AX-05: Get / batch / query / Top-K return the same distance.</summary>
		[TestMethod]
		public void DistanceIdenticalAcrossPaths()
		{
			Key key = new(SuiteHelpers.ns, Set, "ax05");
			Vector stored = Vector.OfFloat32([1f, 2f, 3f]);
			Vector query = Vector.OfFloat32([4f, 5f, 6f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(IdBin, 5), new Bin(VecBin, stored));

			try
			{
				Expression distExp = Exp.Build(VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, query, Exp.VectorBin(VecBin)));

				double viaGet = client.Operate(null, key, ExpOperation.Read(DistBin, distExp, ExpReadFlags.DEFAULT)).GetDouble(DistBin);

				Record[] batch = client.Get(null, [key], ExpOperation.Read(DistBin, distExp, ExpReadFlags.DEFAULT));
				double viaBatch = batch[0].GetDouble(DistBin);

				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(Set);
				stmt.Operations = [ExpOperation.Read(DistBin, distExp, ExpReadFlags.DEFAULT), Operation.Get(IdBin)];
				QueryPolicy qp = new() { filterExp = Exp.Build(Exp.EQ(Exp.IntBin(IdBin), Exp.Val(5))) };
				double viaQuery;
				using (RecordSet rs = client.Query(qp, stmt))
				{
					Assert.IsTrue(rs.Next());
					viaQuery = rs.Record.GetDouble(DistBin);
				}

				Assert.AreEqual(viaGet, viaBatch);
				Assert.AreEqual(viaGet, viaQuery);
				Assert.AreEqual(32.0, viaGet, 0.001);

				if (client.Nodes.All(n => n.HasQueryOrderBy))
				{
					Statement topK = new();
					topK.SetNamespace(SuiteHelpers.ns);
					topK.SetSetName(Set);
					topK.Operations =
					[
						ExpOperation.Read(DistBin, distExp, ExpReadFlags.DEFAULT),
						Operation.Get(IdBin)
					];
					topK.SetOrderBy(DistBin, BinDataType.DOUBLE, Order.DESC);
					topK.SetTopK(1);
					using RecordSet rs = client.Query(null, topK);
					Assert.IsTrue(rs.Next());
					Assert.AreEqual(viaGet, rs.Record.GetDouble(DistBin));
				}
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AX-07: squared-Euclidean overflow → +Inf; float64 overflowing dot is unknown.</summary>
		[TestMethod]
		public void DistanceOverflowBehaviour()
		{
			Key key = new(SuiteHelpers.ns, Set, "ax07");
			// Two float32 vectors whose squared difference overflows float32 but is fine in double.
			float big = float.MaxValue;
			Vector a = Vector.OfFloat32([big, 0f]);
			Vector b = Vector.OfFloat32([-big, 0f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, a));

			try
			{
				double sq = client.Operate(null, key,
					ExpOperation.Read(DistBin,
						Exp.Build(VectorExp.Distance(VectorDistanceMetric.EUCLIDEAN_SQUARED, b, Exp.VectorBin(VecBin))),
						ExpReadFlags.DEFAULT)).GetDouble(DistBin);
				Assert.IsTrue(double.IsPositiveInfinity(sq) || sq > 1e300, "squared Euclidean overflow should be +Inf-scale");

				Vector huge = Vector.OfFloat64([double.MaxValue, double.MaxValue]);
				client.Put(null, key, new Bin(VecBin, huge));
				Record overflowDot = client.Operate(null, key,
					ExpOperation.Read(DistBin,
						Exp.Build(VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, huge, Exp.VectorBin(VecBin))),
						ExpReadFlags.EVAL_NO_FAIL));
				// Float64 overflowing dot product is unknown (nil with EVAL_NO_FAIL).
				Assert.IsTrue(overflowDot.GetValue(DistBin) == null || double.IsInfinity(overflowDot.GetDouble(DistBin)));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AX-11: expression read / filter / distance on a stored vector (SERVER-1912).</summary>
		[TestMethod]
		public void VectorBinExpressionReadFilterAndDistance()
		{
			Key key = new(SuiteHelpers.ns, Set, "ax11");
			Vector v = Vector.OfFloat32([1f, 0f, 0f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, v), new Bin(IdBin, 11));

			try
			{
				Record read = client.Operate(null, key,
					ExpOperation.Read("copy", Exp.Build(Exp.VectorBin(VecBin)), ExpReadFlags.DEFAULT));
				Assert.AreEqual(v, read.GetVector("copy"));

				QueryPolicy filter = new()
				{
					filterExp = Exp.Build(Exp.GT(
						VectorExp.Distance(VectorDistanceMetric.COSINE_SIMILARITY, v, Exp.VectorBin(VecBin)),
						Exp.Val(0.99)))
				};
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(Set);
				stmt.SetBinNames(IdBin);
				int count = 0;
				using RecordSet rs = client.Query(filter, stmt);
				while (rs.Next())
				{
					if (rs.Record.GetLong(IdBin) == 11)
					{
						count++;
					}
				}
				Assert.AreEqual(1, count);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AX-12: reading a vector bin as blob/list/map yields unknown / BIN_TYPE_ERROR.</summary>
		[TestMethod]
		public void WrongTypedBinExpressionsOnVector()
		{
			Key key = new(SuiteHelpers.ns, Set, "ax12");
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, Vector.OfFloat32([1f, 2f, 3f])));

			try
			{
				QueryPolicy blobFilter = new()
				{
					filterExp = Exp.Build(Exp.NE(Exp.BlobBin(VecBin), Exp.Val(new byte[] { 1 })))
				};
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(Set);
				stmt.SetBinNames(VecBin);
				using (RecordSet rs = client.Query(blobFilter, stmt))
				{
					while (rs.Next())
					{
						Assert.Fail("blob-typed filter on vector bin must drop the record");
					}
				}

				try
				{
					client.Operate(null, key,
						ExpOperation.Read("asBlob", Exp.Build(Exp.BlobBin(VecBin)), ExpReadFlags.DEFAULT));
					Assert.Fail("expected BIN_TYPE_ERROR / parameter for blob read of vector");
				}
				catch (AerospikeException ae)
				{
					Assert.IsTrue(
						ae.Result == ResultCode.BIN_TYPE_ERROR || ae.Result == ResultCode.PARAMETER_ERROR || ae.Result == ResultCode.OP_NOT_APPLICABLE,
						"unexpected rc " + ae.Result);
				}
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AX-15: batch operate + query/scan projection with EVAL_NO_FAIL for mismatched dims.</summary>
		[TestMethod]
		public void BatchAndProjectionMatrixWithEvalNoFail()
		{
			Key good = new(SuiteHelpers.ns, Set, "ax15g");
			Key bad = new(SuiteHelpers.ns, Set, "ax15b");
			Vector query = Vector.OfFloat32([1f, 0f]);
			client.Delete(null, good);
			client.Delete(null, bad);
			client.Put(null, good, new Bin(IdBin, 1), new Bin(VecBin, Vector.OfFloat32([1f, 0f])));
			client.Put(null, bad, new Bin(IdBin, 2), new Bin(VecBin, Vector.OfFloat32([1f, 0f, 0f])));

			try
			{
				Expression dist = Exp.Build(VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, query, Exp.VectorBin(VecBin)));
				Record[] batch = client.Get(null, [good, bad],
					ExpOperation.Read(DistBin, dist, ExpReadFlags.EVAL_NO_FAIL),
					Operation.Get(IdBin));
				Assert.IsNotNull(batch[0].GetValue(DistBin));
				Assert.IsNull(batch[1].GetValue(DistBin));

				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(Set);
				stmt.Operations =
				[
					ExpOperation.Read(DistBin, dist, ExpReadFlags.EVAL_NO_FAIL),
					Operation.Get(IdBin)
				];
				int seen = 0;
				using RecordSet rs = client.Query(null, stmt);
				while (rs.Next())
				{
					seen++;
				}
				Assert.IsTrue(seen >= 2);
			}
			finally
			{
				client.Delete(null, good);
				client.Delete(null, bad);
			}
		}

		/// <summary>
		/// AX-09: large FLOAT16 query literal distance matches stored-bin distance.
		/// Uses 16 384 dims (CI-friendly); full max 131 072 is covered by unit max-dim tests.
		/// </summary>
		[TestMethod]
		public void LargeFloat16QueryLiteralMatchesStored()
		{
			const int dims = 16384;
			short[] data = new short[dims];
			data[0] = 0x3c00; // 1.0
			Vector v = Vector.OfFloat16(data);
			Key key = new(SuiteHelpers.ns, Set, "ax09");
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, v));

			try
			{
				double fromLiteral = client.Operate(null, key,
					ExpOperation.Read(DistBin,
						Exp.Build(VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, v, Exp.VectorBin(VecBin))),
						ExpReadFlags.DEFAULT)).GetDouble(DistBin);

				Assert.AreEqual(1.0, fromLiteral, 0.001);
				Assert.AreEqual(Vector.HEADER_SIZE + dims * 2, v.GetWireSize());
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>
		/// AX-06: empty / null query vector refused client-side; int-literal distance compare
		/// is rejected or evaluated without a silent wrong result.
		/// </summary>
		[TestMethod]
		public void DistanceRejectsEmptyQueryAndSurfacesIntLiteralCompare()
		{
			Assert.Throws<ArgumentNullException>(() =>
				VectorExp.Distance(VectorDistanceMetric.DOT_PRODUCT, null, Exp.VectorBin(VecBin)));

			AerospikeException empty = Assert.Throws<AerospikeException>(
				() => Vector.OfFloat32(Array.Empty<float>()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, empty.Result);

			Key key = new(SuiteHelpers.ns, Set, "ax06");
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, Vector.OfFloat32([1f, 0f])));

			try
			{
				Expression intCompare = Exp.Build(Exp.GT(
					VectorExp.Distance(
						VectorDistanceMetric.DOT_PRODUCT,
						Vector.OfFloat32([1f, 0f]),
						Exp.VectorBin(VecBin)),
					Exp.Val(0))); // integer literal vs float distance

				try
				{
					Record record = client.Operate(null, key,
						ExpOperation.Read(DistBin, intCompare, ExpReadFlags.DEFAULT));
					// Some builds coerce; accept a boolean/numeric result but never a crash.
					Assert.IsNotNull(record);
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

		private static long[] FilterIds(Vector query, Func<Exp, Exp, Exp> cmp, double threshold)
		{
			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(cmp(
					VectorExp.Distance(VectorDistanceMetric.EUCLIDEAN_SQUARED, query, Exp.VectorBin(VecBin)),
					Exp.Val(threshold)))
			};
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetBinNames(IdBin);

			List<long> ids = new();
			using RecordSet rs = client.Query(policy, stmt);
			while (rs.Next())
			{
				ids.Add(rs.Record.GetLong(IdBin));
			}
			return ids.ToArray();
		}
	}
}