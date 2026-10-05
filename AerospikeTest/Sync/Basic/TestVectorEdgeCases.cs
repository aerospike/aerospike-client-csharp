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
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestVectorEdgeCases
	{
		[TestMethod]
		public void EmptyVectorsAreAllowedLocally()
		{
			// Cross-client rule: permissive construction/decode; server rejects zero-dim writes.
			AssertRoundTrip(Vector.OfFloat16(Array.Empty<short>()));
			AssertRoundTrip(Vector.OfInt32(Array.Empty<int>()));
			AssertRoundTrip(Vector.OfFloat32(Array.Empty<float>()));
			AssertRoundTrip(Vector.OfFloat64(Array.Empty<double>()));
		}

		[TestMethod]
		public void NullVectorDataIsRejected()
		{
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat16(null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfInt32(null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat32(null));
			Assert.Throws<ArgumentNullException>(() => Vector.OfFloat64(null));
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
		public void Float32SpecialValuesRoundTrip()
		{
			float[] data = new float[]
			{
				float.NaN, float.PositiveInfinity, float.NegativeInfinity,
				-0.0f, 0.0f, float.Epsilon, float.MaxValue
			};
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
		public void Float64SpecialValuesRoundTrip()
		{
			double[] data = new double[]
			{
				double.NaN, double.PositiveInfinity, double.NegativeInfinity,
				-0.0, 0.0, double.Epsilon, double.MaxValue
			};
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
		public void EqualsUsesBitSemanticsForFloats()
		{
			Assert.AreEqual(Vector.OfFloat32(new float[] { float.NaN }), Vector.OfFloat32(new float[] { float.NaN }));
			Assert.AreNotEqual(Vector.OfFloat32(new float[] { -0.0f }), Vector.OfFloat32(new float[] { 0.0f }));
			Assert.AreEqual(Vector.OfFloat64(new double[] { double.NaN }), Vector.OfFloat64(new double[] { double.NaN }));
			Assert.AreNotEqual(Vector.OfFloat64(new double[] { -0.0 }), Vector.OfFloat64(new double[] { 0.0 }));
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

			Assert.Throws<ArgumentException>(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void FromHugeDimensionsThrows()
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(unchecked((uint)int.MaxValue), buffer, 2);

			Assert.Throws<ArgumentException>(() => Vector.From(buffer, 0, buffer.Length));
		}

		[TestMethod]
		public void FromPreservesUnknownVersion()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			buffer[0] = 0x02;

			Vector parsed = Vector.From(buffer, 0, buffer.Length);
			Assert.AreEqual(2, parsed.version);
			Assert.AreNotEqual(v, parsed);
			CollectionAssert.AreEqual(buffer, parsed.GetWireBytes());
		}

		[TestMethod]
		public void RecordGetVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Record record = new Record(new Dictionary<string, object> { { "vecbin", v } }, 1, 0);

			Assert.AreSame(v, record.GetVector("vecbin"));
			Assert.IsNull(record.GetVector("missing"));
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
		public void VectorAsMapValueRoundTrips()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Dictionary<object, object> map = new() { { "k", Value.Get(v) } };

			byte[] packed = Packer.Pack(map, MapOrder.UNORDERED);
			Unpacker unpacker = new Unpacker(packed, 0, packed.Length, false);
			IDictionary outMap = (IDictionary)unpacker.UnpackObject();

			Assert.AreEqual(1, outMap.Count);
			Assert.IsInstanceOfType(outMap["k"], typeof(Vector));
			Assert.AreEqual(v, outMap["k"]);
		}

		[TestMethod]
		public void RawVectorInMapBinPacksSuccessfully()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Bin bin = new Bin("vecmap", new Dictionary<object, object> { { "k", v } });

			byte[] buffer = new byte[bin.value.EstimateSize()];
			int written = bin.value.Write(buffer, 0);
			Assert.AreEqual(buffer.Length, written);
		}

		[TestMethod]
		public void NestedListOfMapOfVectorRoundTrips()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.5f });
			Dictionary<object, object> inner = new() { { "v", Value.Get(v) } };
			List<object> list = new() { inner };

			byte[] packed = Packer.Pack(list);
			Unpacker unpacker = new Unpacker(packed, 0, packed.Length, false);
			IList outList = (IList)unpacker.UnpackObject();
			IDictionary outMap = (IDictionary)outList[0];
			Assert.AreEqual(v, outMap["v"]);
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
