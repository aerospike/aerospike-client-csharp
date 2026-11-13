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
	/// AX-01 / AX-10 / AX-13: distance reference values and per-element-type tolerance.
	/// Requires server version 8.2.1+.
	/// </summary>
	[TestClass]
	public class TestVectorDistanceReference : TestSync
	{
		private const string SetName = "vecDistRef";
		private const string VecBin = "v";
		private const string DistBin = "d";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();
		}

		/// <summary>
		/// AX-01: PRD example [1,2,3] vs [4,5,6] → dot 32, squared Euclidean 27, cosine ≈ 0.9746.
		/// </summary>
		[TestMethod]
		public void PrdExampleDistances()
		{
			Vector a = Vector.OfFloat32([1f, 2f, 3f]);
			Vector b = Vector.OfFloat32([4f, 5f, 6f]);

			AssertWithin(32.0, ServerDistance(VectorDistanceMetric.DOT_PRODUCT, a, b), 0.001);
			AssertWithin(27.0, ServerDistance(VectorDistanceMetric.EUCLIDEAN_SQUARED, a, b), 0.001);
			AssertWithin(32.0 / Math.Sqrt(14.0 * 77.0), ServerDistance(VectorDistanceMetric.COSINE_SIMILARITY, a, b), 0.001);

			AssertOneHotPair(3, 0, 1);
			AssertOneHotPair(8, 2, 5);
			AssertOneHotPair(33, 0, 32);

			Vector zero = Vector.OfFloat32([0f, 0f, 0f]);
			AssertWithin(0.0, ServerDistance(VectorDistanceMetric.EUCLIDEAN_SQUARED, zero, zero), 1e-9);
			AssertWithin(0.0, ServerDistance(VectorDistanceMetric.DOT_PRODUCT, zero, a), 1e-9);
		}

		/// <summary>AX-10: each element type stays within 0.1% of a double reference.</summary>
		[TestMethod]
		public void DistanceWithinPointOnePercentPerElementType()
		{
			AssertTypeTolerance(
				Vector.OfFloat16([(Half)1f, (Half)2f, (Half)3f]),
				Vector.OfFloat16([(Half)4f, (Half)5f, (Half)6f]),
				[1.0, 2.0, 3.0],
				[4.0, 5.0, 6.0]);

			AssertTypeTolerance(
				Vector.OfInt32([1, 2, 3]),
				Vector.OfInt32([4, 5, 6]),
				[1.0, 2.0, 3.0],
				[4.0, 5.0, 6.0]);

			AssertTypeTolerance(
				Vector.OfFloat32([1f, 2f, 3f]),
				Vector.OfFloat32([4f, 5f, 6f]),
				[1.0, 2.0, 3.0],
				[4.0, 5.0, 6.0]);

			AssertTypeTolerance(
				Vector.OfFloat64([1.0, 2.0, 3.0]),
				Vector.OfFloat64([4.0, 5.0, 6.0]),
				[1.0, 2.0, 3.0],
				[4.0, 5.0, 6.0]);
		}

		/// <summary>
		/// AX-13: default precision — FLOAT16/32/INT32 match float32 reference; FLOAT64 matches double.
		/// </summary>
		[TestMethod]
		public void DefaultPrecisionMatchesFloatOrDoubleReference()
		{
			float[] af = [1f, 2f, 3f];
			float[] bf = [4f, 5f, 6f];
			double[] ad = [1.0, 2.0, 3.0];
			double[] bd = [4.0, 5.0, 6.0];

			AssertFloat32Reference(Vector.OfFloat16([(Half)1f, (Half)2f, (Half)3f]), Vector.OfFloat16([(Half)4f, (Half)5f, (Half)6f]), af, bf);
			AssertFloat32Reference(Vector.OfFloat32(af), Vector.OfFloat32(bf), af, bf);
			AssertFloat32Reference(Vector.OfInt32([1, 2, 3]), Vector.OfInt32([4, 5, 6]), af, bf);

			foreach (VectorDistanceMetric metric in AllMetrics())
			{
				double expected = Reference(metric, ad, bd);
				double actual = ServerDistance(metric, Vector.OfFloat64(ad), Vector.OfFloat64(bd));
				AssertWithin(expected, actual, RelativeTol(expected));
			}
		}

		private static void AssertOneHotPair(int dims, int i, int j)
		{
			float[] a = new float[dims];
			float[] b = new float[dims];
			a[i] = 1f;
			b[j] = 1f;

			Vector va = Vector.OfFloat32(a);
			Vector vb = Vector.OfFloat32(b);

			AssertWithin(0.0, ServerDistance(VectorDistanceMetric.DOT_PRODUCT, va, vb), 1e-6);
			AssertWithin(2.0, ServerDistance(VectorDistanceMetric.EUCLIDEAN_SQUARED, va, vb), 1e-6);
			AssertWithin(0.0, ServerDistance(VectorDistanceMetric.COSINE_SIMILARITY, va, vb), 1e-6);
		}

		private static void AssertTypeTolerance(Vector query, Vector stored, double[] a, double[] b)
		{
			foreach (VectorDistanceMetric metric in AllMetrics())
			{
				double expected = Reference(metric, a, b);
				double actual = ServerDistance(metric, query, stored);
				AssertWithin(expected, actual, Math.Max(Math.Abs(expected) * 0.001, 1e-9));
			}
		}

		private static void AssertFloat32Reference(Vector query, Vector stored, float[] a, float[] b)
		{
			double[] ad = [a[0], a[1], a[2]];
			double[] bd = [b[0], b[1], b[2]];

			foreach (VectorDistanceMetric metric in AllMetrics())
			{
				// Float32 reference path: cast products to float then widen.
				double expected = ReferenceFloat32(metric, a, b);
				double actual = ServerDistance(metric, query, stored);
				AssertWithin(expected, actual, Math.Max(Math.Abs(expected) * 1e-5, 1e-6),
					metric + " should match float32 reference (got " + actual + ", expected " + expected + ")");
				// Also stay near the double reference (AX-10-style).
				AssertWithin(Reference(metric, ad, bd), actual, Math.Max(Math.Abs(Reference(metric, ad, bd)) * 0.001, 1e-9));
			}
		}

		private static double ServerDistance(VectorDistanceMetric metric, Vector query, Vector stored)
		{
			Key key = new(SuiteHelpers.ns, SetName, "ref_" + Guid.NewGuid().ToString("N"));

			try
			{
				client.Put(null, key, new Bin(VecBin, stored));
				Record record = client.Operate(null, key,
					ExpOperation.Read(DistBin,
						Exp.Build(VectorExp.Distance(metric, query, Exp.VectorBin(VecBin))),
						ExpReadFlags.DEFAULT));
				AssertRecordFound(key, record);
				return record.GetDouble(DistBin);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		private static double Reference(VectorDistanceMetric metric, double[] a, double[] b)
		{
			double dot = 0;
			double na = 0;
			double nb = 0;
			double sq = 0;

			for (int i = 0; i < a.Length; i++)
			{
				dot += a[i] * b[i];
				na += a[i] * a[i];
				nb += b[i] * b[i];
				double d = a[i] - b[i];
				sq += d * d;
			}

			return metric switch
			{
				VectorDistanceMetric.DOT_PRODUCT => dot,
				VectorDistanceMetric.EUCLIDEAN_SQUARED => sq,
				VectorDistanceMetric.COSINE_SIMILARITY => dot / Math.Sqrt(na * nb),
				_ => throw new ArgumentOutOfRangeException(nameof(metric))
			};
		}

		private static double ReferenceFloat32(VectorDistanceMetric metric, float[] a, float[] b)
		{
			float dot = 0;
			float na = 0;
			float nb = 0;
			float sq = 0;

			for (int i = 0; i < a.Length; i++)
			{
				dot += a[i] * b[i];
				na += a[i] * a[i];
				nb += b[i] * b[i];
				float d = a[i] - b[i];
				sq += d * d;
			}

			return metric switch
			{
				VectorDistanceMetric.DOT_PRODUCT => dot,
				VectorDistanceMetric.EUCLIDEAN_SQUARED => sq,
				VectorDistanceMetric.COSINE_SIMILARITY => dot / Math.Sqrt((double)na * nb),
				_ => throw new ArgumentOutOfRangeException(nameof(metric))
			};
		}

		private static VectorDistanceMetric[] AllMetrics() =>
		[
			VectorDistanceMetric.EUCLIDEAN_SQUARED,
			VectorDistanceMetric.DOT_PRODUCT,
			VectorDistanceMetric.COSINE_SIMILARITY
		];

		private static double RelativeTol(double expected) =>
			Math.Max(Math.Abs(expected) * 0.001, 1e-9);

		private static void AssertWithin(double expected, double actual, double tol, string message = null)
		{
			Assert.IsTrue(Math.Abs(actual - expected) <= tol,
				message ?? $"expected {expected} ± {tol}, got {actual}");
		}
	}
}
