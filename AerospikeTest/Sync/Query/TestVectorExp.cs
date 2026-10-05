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
	/// Vector-distance expression tests: filtering and Top-K over projected distance.
	/// </summary>
	[TestClass]
	public class TestVectorExp : TestSync
	{
		private const string VecBin = "embedding";
		private const string IdBin = "id";
		private const string KeyPrefix = "vecexp";
		private const string SetName = "vectorExpSet";
		private const int Size = 20;
		private const int Dims = 4;

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			for (int i = 0; i < Size; i++)
			{
				Key key = new(SuiteHelpers.ns, SetName, KeyPrefix + i);
				float[] data = new float[Dims];

				for (int d = 0; d < Dims; d++)
				{
					data[d] = i + d * 0.1f;
				}
				client.Put(null, key, new Bin(IdBin, i), new Bin(VecBin, Vector.OfFloat32(data)));
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			for (int i = 0; i < Size; i++)
			{
				client.Delete(null, new Key(SuiteHelpers.ns, SetName, KeyPrefix + i));
			}
		}

		private static Vector Query(int baseId)
		{
			float[] data = new float[Dims];

			for (int d = 0; d < Dims; d++)
			{
				data[d] = baseId + d * 0.1f;
			}
			return Vector.OfFloat32(data);
		}

		[TestMethod]
		public void FilterByEuclideanDistance()
		{
			AssertFilterIds(VectorDistanceMetric.EUCLIDEAN, false, 0.01, 5);
		}

		[TestMethod]
		public void FilterByDotProductDistance()
		{
			AssertFilterIds(VectorDistanceMetric.DOT_PRODUCT, true, 200.0, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
		}

		[TestMethod]
		public void FilterByCosineDistance()
		{
			AssertFilterIds(VectorDistanceMetric.COSINE, true, 0.999999, 5);
		}

		[TestMethod]
		public void DistanceWrongParticleTypeIsUnknown()
		{
			AssertUnknownDistance("wrongtype", "not-a-vector");
		}

		[TestMethod]
		public void DistanceMismatchedDimensionsIsUnknown()
		{
			AssertUnknownDistance("dimensions", Vector.OfFloat32(new float[] { 0, 0, 0 }));
		}

		[TestMethod]
		public void DistanceMismatchedElementTypeIsUnknown()
		{
			AssertUnknownDistance("elementtype", Vector.OfInt32(new int[] { 0, 0, 0, 0 }));
		}

		private static void AssertFilterIds(VectorDistanceMetric metric, bool greaterThan, double threshold, params long[] expectedIds)
		{
			QueryPolicy policy = new();
			Exp distance = VectorExp.Distance(metric, Query(5), Exp.VectorBin(VecBin));
			policy.filterExp = Exp.Build(greaterThan ?
				Exp.GT(distance, Exp.Val(threshold)) :
				Exp.LT(distance, Exp.Val(threshold)));

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SetName);

			RecordSet rs = client.Query(policy, stmt);
			HashSet<long> actual = new();

			try
			{
				while (rs.Next())
				{
					actual.Add(rs.Record.GetLong(IdBin));
				}
			}
			finally
			{
				rs.Close();
			}

			HashSet<long> expected = new(expectedIds);
			Assert.IsTrue(expected.SetEquals(actual),
				$"Expected [{string.Join(", ", expectedIds)}], got [{string.Join(", ", actual)}]");
		}

		private static void AssertUnknownDistance(string suffix, object vector)
		{
			Key key = new(SuiteHelpers.ns, SetName, KeyPrefix + '_' + suffix);

			try
			{
				client.Put(null, key, new Bin(VecBin, Value.Get(vector)));

				Record record = client.Operate(null, key,
					ExpOperation.Read("distance",
						Exp.Build(VectorExp.Distance(VectorDistanceMetric.EUCLIDEAN, Query(0), Exp.VectorBin(VecBin))),
						ExpReadFlags.EVAL_NO_FAIL));

				Assert.IsNull(record.GetValue("distance"));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		[TestMethod]
		public void VectorSearchTopKNearest()
		{
			int k = 5;
			string distBin = "dist";

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SetName);

			stmt.Operations =
			[
				Operation.Get(IdBin),
				ExpOperation.Read(distBin,
					Exp.Build(VectorExp.Distance(VectorDistanceMetric.EUCLIDEAN, Query(0), Exp.VectorBin(VecBin))),
					ExpReadFlags.DEFAULT)
			];
			stmt.SetOrderBy(distBin, BinDataType.DOUBLE, Order.ASC);
			stmt.SetTopK(k);

			RecordSet rs = client.Query(null, stmt);
			int count = 0;
			double previous = double.NegativeInfinity;

			try
			{
				while (rs.Next())
				{
					Record record = rs.Record;
					double dist = record.GetDouble(distBin);
					Assert.IsTrue(dist >= previous, "Top-K nearest must be in ascending distance order");
					Assert.AreEqual(count, record.GetLong(IdBin), "Top-K nearest must return records nearest to query(0)");
					previous = dist;
					count++;
				}
			}
			finally
			{
				rs.Close();
			}

			Assert.AreEqual(k, count);
		}
	}
}
