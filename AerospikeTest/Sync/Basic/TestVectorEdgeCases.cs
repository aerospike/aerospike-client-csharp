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
using System.Collections;
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Client-side vector edge-case unit tests (no live cluster required).
	/// </summary>
	[TestClass]
	public class TestVectorEdgeCases
	{
		[TestMethod]
		public void EmptyVectorsAreRejected()
		{
			AssertParseError(() => Vector.OfFloat16(Array.Empty<short>()));
			AssertParseError(() => Vector.OfFloat16(Array.Empty<Half>()));
			AssertParseError(() => Vector.OfFloat16(Array.Empty<float>()));
			AssertParseError(() => Vector.OfInt32(Array.Empty<int>()));
			AssertParseError(() => Vector.OfFloat32(Array.Empty<float>()));
			AssertParseError(() => Vector.OfFloat64(Array.Empty<double>()));
		}

		[TestMethod]
		public void FromZeroDimensionsThrows()
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(0, buffer, 2);
			AssertParseError(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void NullVectorDataIsRejected()
		{
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat16((short[])null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat16((Half[])null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat16((float[])null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfInt32(null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat32(null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat64(null));
		}

		[TestMethod]
		public void Float16HalfRejectsNonFinite()
		{
			AssertParseError(() => Vector.OfFloat16(new Half[] { Half.NaN }));
			AssertParseError(() => Vector.OfFloat16(new Half[] { Half.PositiveInfinity }));
			AssertParseError(() => Vector.OfFloat16(new Half[] { Half.NegativeInfinity }));
		}

		[TestMethod]
		public void Float16FromFloatRejectsNonFiniteAndOverflow()
		{
			AssertParseError(() => Vector.OfFloat16(new float[] { float.NaN }));
			AssertParseError(() => Vector.OfFloat16(new float[] { float.PositiveInfinity }));
			AssertParseError(() => Vector.OfFloat16(new float[] { float.NegativeInfinity }));
			// Magnitudes above 65520 convert to infinity in binary16.
			AssertParseError(() => Vector.OfFloat16(new float[] { 65521f }));
			AssertParseError(() => Vector.OfFloat16(new float[] { -65521f }));
		}

		[TestMethod]
		public void SingleElementVectorsRoundTrip()
		{
			AssertRoundTrip(Vector.OfFloat16(new short[] { 0x3c00 }));
			AssertRoundTrip(Vector.OfInt32(new int[] { 42 }));
			AssertRoundTrip(Vector.OfFloat32(new float[] { 1.5f }));
			AssertRoundTrip(Vector.OfFloat64(new double[] { 1.5 }));
		}

		[TestMethod]
		public void Int32Extremes()
		{
			int[] data = new int[] { int.MinValue, int.MaxValue, 0, -1 };
			Vector v = Vector.OfInt32(data);

			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			Vector parsed = Vector.From(buffer, 0, buffer.Length);
			CollectionAssert.AreEqual(data, parsed.GetInt32Data());
		}

		[TestMethod]
		public void Float32FiniteExtremesRoundTrip()
		{
			float[] data = new float[] { -0.0f, 0.0f, float.Epsilon, float.MinValue, float.MaxValue };
			Vector v = Vector.OfFloat32(data);

			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			float[] outData = Vector.From(buffer, 0, buffer.Length).GetFloat32Data();
			for (int i = 0; i < data.Length; i++)
			{
				Assert.AreEqual(BitConverter.SingleToInt32Bits(data[i]), BitConverter.SingleToInt32Bits(outData[i]));
			}
		}

		[TestMethod]
		public void Float32RejectsNonFinite()
		{
			AssertParseError(() => Vector.OfFloat32(new float[] { float.NaN }));
			AssertParseError(() => Vector.OfFloat32(new float[] { float.PositiveInfinity }));
			AssertParseError(() => Vector.OfFloat32(new float[] { float.NegativeInfinity }));
		}

		[TestMethod]
		public void Float64FiniteExtremesRoundTrip()
		{
			double[] data = new double[] { -0.0, 0.0, double.Epsilon, double.MinValue, double.MaxValue };
			Vector v = Vector.OfFloat64(data);

			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			double[] outData = Vector.From(buffer, 0, buffer.Length).GetFloat64Data();
			for (int i = 0; i < data.Length; i++)
			{
				Assert.AreEqual(BitConverter.DoubleToInt64Bits(data[i]), BitConverter.DoubleToInt64Bits(outData[i]));
			}
		}

		[TestMethod]
		public void Float64RejectsNonFinite()
		{
			AssertParseError(() => Vector.OfFloat64(new double[] { double.NaN }));
			AssertParseError(() => Vector.OfFloat64(new double[] { double.PositiveInfinity }));
			AssertParseError(() => Vector.OfFloat64(new double[] { double.NegativeInfinity }));
		}

		[TestMethod]
		public void EqualsUsesBitSemanticsForFloats()
		{
			Assert.AreNotEqual(Vector.OfFloat32(new float[] { -0.0f }), Vector.OfFloat32(new float[] { 0.0f }));
			Assert.AreNotEqual(Vector.OfFloat64(new double[] { -0.0 }), Vector.OfFloat64(new double[] { 0.0 }));
		}

		[TestMethod]
		public void OversizedDimensionsAreRejected()
		{
			// Prefer From() header checks so we don't allocate MaxDimensions+1 element arrays.
			AssertFromDimensionsRejected(Vector.ElementType.FLOAT16, Vector.MaxDimensionsFloat16 + 1);
			AssertFromDimensionsRejected(Vector.ElementType.INT32, Vector.MaxDimensionsInt32OrFloat32 + 1);
			AssertFromDimensionsRejected(Vector.ElementType.FLOAT32, Vector.MaxDimensionsInt32OrFloat32 + 1);
			AssertFromDimensionsRejected(Vector.ElementType.FLOAT64, Vector.MaxDimensionsFloat64 + 1);

			// Of* shares RequireValidDimensions; spot-check one constructor path.
			AssertParseError(() => Vector.OfFloat64(new double[Vector.MaxDimensionsFloat64 + 1]));
		}

		/// <summary>AV-03: 1 and per-type maximum dimensions are accepted; 0 / max+1 refused.</summary>
		[TestMethod]
		public void MaxAndMinDimensionsAccepted()
		{
			Assert.AreEqual(1, Vector.OfFloat16(new short[1]).dimensions);
			Assert.AreEqual(1, Vector.OfInt32(new int[1]).dimensions);
			Assert.AreEqual(1, Vector.OfFloat32(new float[1]).dimensions);
			Assert.AreEqual(1, Vector.OfFloat64(new double[1]).dimensions);

			AssertAcceptedFrom(Vector.ElementType.FLOAT16, Vector.MaxDimensionsFloat16);
			AssertAcceptedFrom(Vector.ElementType.INT32, Vector.MaxDimensionsInt32OrFloat32);
			AssertAcceptedFrom(Vector.ElementType.FLOAT32, Vector.MaxDimensionsInt32OrFloat32);
			AssertAcceptedFrom(Vector.ElementType.FLOAT64, Vector.MaxDimensionsFloat64);

			Assert.AreEqual(Vector.MaxDimensionsFloat64, Vector.OfFloat64(new double[Vector.MaxDimensionsFloat64]).dimensions);
		}

		/// <summary>
		/// AV-05: OfFloat16(float[]) matches round-ties-to-even for every finite float16 pattern.
		/// </summary>
		[TestMethod]
		public void Float16FromFloatMatchesAllFiniteBitPatterns()
		{
			int finite = 0;

			for (int i = 0; i < 65536; i++)
			{
				ushort bits = (ushort)i;
				Half half = BitConverter.UInt16BitsToHalf(bits);
				if (!Half.IsFinite(half))
				{
					continue;
				}

				finite++;
				float asFloat = (float)half;
				short[] data = Vector.OfFloat16([asFloat]).GetFloat16Data();
				Assert.AreEqual(unchecked((short)bits), data[0],
					"float16 bits 0x" + bits.ToString("x4") + " via float " + asFloat);
			}

			Assert.AreEqual(63488, finite);
		}

		/// <summary>AV-12: C# has no OfFloat32(double[]); callers narrow first. Infinity refused.</summary>
		[TestMethod]
		public void Float32HasNoDoubleArrayFactoryAndRejectsInfinity()
		{
			Assert.IsNull(typeof(Vector).GetMethod("OfFloat32", [typeof(double[])]));

			// Language narrowing before OfFloat32: 16777217 → 16777216 (nearest even float32).
			float narrowed = (float)16777217.0;
			Assert.AreEqual(16777216f, narrowed);
			Assert.AreEqual(16777216f, Vector.OfFloat32([narrowed]).GetFloat32Data()[0]);

			AssertParseError(() => Vector.OfFloat32([float.PositiveInfinity]));
		}

		/// <summary>
		/// AV-09: a vector cannot be used as a record key — refused before send.
		/// </summary>
		[TestMethod]
		public void VectorRejectedAsRecordKey()
		{
			Vector v = Vector.OfFloat32([1f, 2f]);
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => new Key("test", "set", Value.Get(v)));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			StringAssert.Contains(ae.BaseMessage, "Invalid key type: Vector");
		}

		/// <summary>
		/// AV-10: INT32 extremes and finite FLOAT16 bit patterns accepted; numeric
		/// factories refuse non-finite values (raw short[] bit interchange stays exact).
		/// </summary>
		[TestMethod]
		public void Int32AndFloat16BitPatternBounds()
		{
			CollectionAssert.AreEqual(
				new int[] { int.MinValue, int.MaxValue },
				Vector.OfInt32([int.MinValue, int.MaxValue]).GetInt32Data());

			short[] finiteBits = [0x0000, 0x7bff, unchecked((short)0xfbff)];
			CollectionAssert.AreEqual(finiteBits, Vector.OfFloat16(finiteBits).GetFloat16Data());

			AssertParseError(() => Vector.OfFloat16(new Half[] { Half.NaN }));
			AssertParseError(() => Vector.OfFloat16(new float[] { float.PositiveInfinity }));
			AssertParseError(() => Vector.OfFloat32([float.NaN]));
			AssertParseError(() => Vector.OfFloat64([double.NegativeInfinity]));
		}

		[TestMethod]
		public void Float16SpecialBitPatternsRoundTrip()
		{
			short[] data = new short[]
			{
				0x7c00, unchecked((short)0xfc00), 0x7e00, 0x0000, unchecked((short)0x8000), 0x0001
			};
			Vector v = Vector.OfFloat16(data);

			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			CollectionAssert.AreEqual(data, Vector.From(buffer, 0, buffer.Length).GetFloat16Data());
		}

		[TestMethod]
		public void GetElementBytesMatchesWriteToWithoutHeader()
		{
			AssertElementBytes(Vector.OfFloat16(new short[] { 0x3c00, unchecked((short)0xbc00), 0x4000 }));
			AssertElementBytes(Vector.OfInt32(new int[] { -1, 0, 1, 12345 }));
			AssertElementBytes(Vector.OfFloat32(new float[] { 1.5f, -2.25f, 3.14159f }));
			AssertElementBytes(Vector.OfFloat64(new double[] { 1.5, -2.25, 3.14159 }));
		}

		[TestMethod]
		public void FromNegativeDimensionsThrows()
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)Vector.ElementType.FLOAT32;
			buffer[2] = 0xff;
			buffer[3] = 0xff;
			buffer[4] = 0xff;
			buffer[5] = 0xff;

			AssertParseError(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void FromHugeDimensionsThrows()
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(unchecked((uint)int.MaxValue), buffer, 2);

			AssertParseError(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void FromRejectsUnknownVersion()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			buffer[0] = 0x02;

			AssertParseError(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void RecordGetVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Record record = new Record(new Dictionary<string, object>
			{
				{ "vecbin", v },
				{ "other", 42L }
			}, 1, 0);

			Assert.AreSame(v, record.GetVector("vecbin"));
			Assert.IsNull(record.GetVector("missing"));
			Assert.IsNull(record.GetVector("other"));
		}

		[TestMethod]
		public void ValueGetObjectAllTypes()
		{
			AssertObjectWrap(Vector.OfFloat16(new short[] { 1, 2 }));
			AssertObjectWrap(Vector.OfInt32(new int[] { 1, 2 }));
			AssertObjectWrap(Vector.OfFloat32(new float[] { 1.0f, 2.0f }));
			AssertObjectWrap(Vector.OfFloat64(new double[] { 1.0, 2.0 }));
		}

		[TestMethod]
		public void PackerRejectsVectorAsMapValue()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Dictionary<object, object> map = new() { { "k", Value.Get(v) } };
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => Packer.Pack(map, MapOrder.UNORDERED));
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains("list/map cannot hold a vector"));
		}

		[TestMethod]
		public void RawVectorInMapBinRejected()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Bin bin = new Bin("vecmap", new Dictionary<object, object> { { "k", v } });
			AerospikeException e = Assert.Throws<AerospikeException>(() => bin.value.EstimateSize());
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains("list/map cannot hold a vector"));
		}

		[TestMethod]
		public void NestedListOfMapOfVectorRejected()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.5f });
			Dictionary<object, object> inner = new() { { "v", Value.Get(v) } };
			List<object> list = new() { inner };

			AerospikeException e = Assert.Throws<AerospikeException>(() => Packer.Pack(list));
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains("list/map cannot hold a vector"));
		}

		private static void AssertParseError(Action action)
		{
			AerospikeException e = Assert.Throws<AerospikeException>(action);
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
		}

		private static void AssertFromDimensionsRejected(Vector.ElementType elementType, int dimensions)
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)elementType;
			ByteUtil.IntToLittleBytes((uint)dimensions, buffer, 2);
			AssertParseError(() => Vector.From(buffer, 0, buffer.Length));
		}

		private static void AssertAcceptedFrom(Vector.ElementType elementType, int dimensions)
		{
			int elemSize = Vector.GetElementByteSize(elementType);
			byte[] buffer = new byte[Vector.HEADER_SIZE + dimensions * elemSize];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)elementType;
			ByteUtil.IntToLittleBytes((uint)dimensions, buffer, 2);
			Vector v = Vector.From(buffer, 0, buffer.Length);
			Assert.AreEqual(dimensions, v.dimensions);
			Assert.AreEqual(elementType, v.elementType);
		}

		private static void AssertRoundTrip(Vector v)
		{
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);
			Assert.AreEqual(v, Vector.From(buffer, 0, buffer.Length));
		}

		private static void AssertElementBytes(Vector v)
		{
			int dataSize = v.dimensions * Vector.GetElementByteSize(v.elementType);
			byte[] elements = v.GetElementBytes();
			Assert.AreEqual(dataSize, elements.Length);

			byte[] full = new byte[v.GetWireSize()];
			v.WriteTo(full, 0);

			CollectionAssert.AreEqual(full.AsSpan(Vector.HEADER_SIZE).ToArray(), elements);
		}

		private static void AssertObjectWrap(Vector v)
		{
			Value value = Value.Get((object)v);
			Assert.IsInstanceOfType(value, typeof(Value.VectorValue));

			byte[] expected = new byte[v.GetWireSize()];
			v.WriteTo(expected, 0);

			byte[] actual = new byte[value.EstimateSize()];
			int written = value.Write(actual, 0);

			Assert.AreEqual(expected.Length, written);
			CollectionAssert.AreEqual(expected, actual);
		}
	}
}
