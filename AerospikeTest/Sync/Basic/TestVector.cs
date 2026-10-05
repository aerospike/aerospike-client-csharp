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
using System.Buffers.Binary;
using System.Collections;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestVector
	{
		//-------------------------------------------------------
		// ElementType
		//-------------------------------------------------------

		[TestMethod]
		public void ElementTypeCodes()
		{
			AssertElementTypeCode(Vector.ElementType.FLOAT16, 0x01);
			AssertElementTypeCode(Vector.ElementType.INT32, 0x02);
			AssertElementTypeCode(Vector.ElementType.FLOAT32, 0x03);
			AssertElementTypeCode(Vector.ElementType.FLOAT64, 0x04);
		}

		private static void AssertElementTypeCode(Vector.ElementType type, byte expected)
		{
			Assert.AreEqual(expected, (byte)type);
		}

		[TestMethod]
		public void ElementTypeByteSizes()
		{
			Assert.AreEqual(2, Vector.GetElementByteSize(Vector.ElementType.FLOAT16));
			Assert.AreEqual(4, Vector.GetElementByteSize(Vector.ElementType.INT32));
			Assert.AreEqual(4, Vector.GetElementByteSize(Vector.ElementType.FLOAT32));
			Assert.AreEqual(8, Vector.GetElementByteSize(Vector.ElementType.FLOAT64));
		}

		[TestMethod]
		public void ElementTypeFromCode()
		{
			foreach (Vector.ElementType type in Enum.GetValues(typeof(Vector.ElementType)))
			{
				Assert.AreEqual(type, Vector.ElementTypeFromCode((byte)type));
			}
		}

		[TestMethod]
		public void ElementTypeFromInvalidCode()
		{
			Assert.Throws<ArgumentException>(() => Vector.ElementTypeFromCode(0x7f));
		}

		//-------------------------------------------------------
		// Vector construction and accessors
		//-------------------------------------------------------

		[TestMethod]
		public void ConstructFloat16()
		{
			short[] data = new short[] { 0x3c00, unchecked((short)0xbc00), 0x4000 };
			Vector v = Vector.OfFloat16(data);

			Assert.AreEqual(Vector.VERSION, v.version);
			Assert.AreEqual(Vector.ElementType.FLOAT16, v.elementType);
			Assert.AreEqual(3, v.dimensions);
			CollectionAssert.AreEqual(data, v.GetFloat16Data());
		}

		[TestMethod]
		public void ConstructInt32()
		{
			int[] data = new int[] { -1, 0, 1, int.MaxValue };
			Vector v = Vector.OfInt32(data);

			Assert.AreEqual(Vector.VERSION, v.version);
			Assert.AreEqual(Vector.ElementType.INT32, v.elementType);
			Assert.AreEqual(4, v.dimensions);
			CollectionAssert.AreEqual(data, v.GetInt32Data());
		}

		[TestMethod]
		public void ConstructFloat32()
		{
			float[] data = new float[] { 1.5f, -2.25f, 0.0f, 3.14159f, float.MaxValue };
			Vector v = Vector.OfFloat32(data);

			Assert.AreEqual(Vector.VERSION, v.version);
			Assert.AreEqual(Vector.ElementType.FLOAT32, v.elementType);
			Assert.AreEqual(5, v.dimensions);
			CollectionAssert.AreEqual(data, v.GetFloat32Data());
		}

		[TestMethod]
		public void ConstructFloat64()
		{
			double[] data = new double[] { 1.5, -2.25, double.MaxValue };
			Vector v = Vector.OfFloat64(data);

			Assert.AreEqual(Vector.VERSION, v.version);
			Assert.AreEqual(Vector.ElementType.FLOAT64, v.elementType);
			Assert.AreEqual(3, v.dimensions);
			CollectionAssert.AreEqual(data, v.GetFloat64Data());
		}

		//-------------------------------------------------------
		// Immutability (defensive copies)
		//-------------------------------------------------------

		[TestMethod]
		public void ConstructorCopiesInput()
		{
			float[] data = new float[] { 1.0f, 2.0f, 3.0f };
			Vector v = Vector.OfFloat32(data);

			data[0] = 99.0f;

			Assert.AreEqual(1.0f, v.GetFloat32Data()[0]);
		}

		[TestMethod]
		public void GetterReturnsCopy()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });

			float[] first = v.GetFloat32Data();
			first[0] = 99.0f;

			Assert.AreEqual(1.0f, v.GetFloat32Data()[0]);
		}

		[TestMethod]
		public void WrongTypeGetterThrows()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f });
			Assert.Throws<InvalidOperationException>(() => v.GetInt32Data());
		}

		//-------------------------------------------------------
		// Wire size
		//-------------------------------------------------------

		[TestMethod]
		public void WireSize()
		{
			Assert.AreEqual(Vector.HEADER_SIZE + 3 * 2, Vector.OfFloat16(new short[3]).GetWireSize());
			Assert.AreEqual(Vector.HEADER_SIZE + 4 * 4, Vector.OfInt32(new int[4]).GetWireSize());
			Assert.AreEqual(Vector.HEADER_SIZE + 5 * 4, Vector.OfFloat32(new float[5]).GetWireSize());
			Assert.AreEqual(Vector.HEADER_SIZE + 2 * 8, Vector.OfFloat64(new double[2]).GetWireSize());
		}

		//-------------------------------------------------------
		// equals / hashCode / toString
		//-------------------------------------------------------

		[TestMethod]
		public void EqualsAndHashCode()
		{
			Vector a = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
			Vector b = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });

			Assert.AreEqual(a, b);
			Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
		}

		[TestMethod]
		public void NotEqualsDifferentData()
		{
			Vector a = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
			Vector b = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 4.0f });

			Assert.AreNotEqual(a, b);
		}

		[TestMethod]
		public void NotEqualsDifferentType()
		{
			Vector a = Vector.OfInt32(new int[] { 1, 2, 3 });
			Vector b = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });

			Assert.AreNotEqual(a, b);
		}

		[TestMethod]
		public void ToStringContainsData()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Assert.AreEqual("[1, 2, 3]", v.ToString());
		}

		//-------------------------------------------------------
		// WriteTo (wire format)
		//-------------------------------------------------------

		[TestMethod]
		public void WriteToFloat32()
		{
			float[] data = new float[] { 1.5f, -2.25f, 3.14159f };
			Vector v = Vector.OfFloat32(data);

			byte[] buffer = new byte[v.GetWireSize()];
			int written = v.WriteTo(buffer, 0);

			Assert.AreEqual(v.GetWireSize(), written);
			AssertHeader(buffer, Vector.ElementType.FLOAT32, data.Length);

			for (int i = 0; i < data.Length; i++)
			{
				float decoded = BinaryPrimitives.ReadSingleLittleEndian(buffer.AsSpan(Vector.HEADER_SIZE + i * 4, 4));
				Assert.AreEqual(data[i], decoded);
			}
		}

		[TestMethod]
		public void WriteToFloat64()
		{
			double[] data = new double[] { 1.5, -2.25, 3.14159 };
			Vector v = Vector.OfFloat64(data);

			byte[] buffer = new byte[v.GetWireSize()];
			int written = v.WriteTo(buffer, 0);

			Assert.AreEqual(v.GetWireSize(), written);
			AssertHeader(buffer, Vector.ElementType.FLOAT64, data.Length);

			for (int i = 0; i < data.Length; i++)
			{
				double decoded = BinaryPrimitives.ReadDoubleLittleEndian(buffer.AsSpan(Vector.HEADER_SIZE + i * 8, 8));
				Assert.AreEqual(data[i], decoded);
			}
		}

		[TestMethod]
		public void WriteToInt32()
		{
			int[] data = new int[] { -1, 0, 1, 12345 };
			Vector v = Vector.OfInt32(data);

			byte[] buffer = new byte[v.GetWireSize()];
			int written = v.WriteTo(buffer, 0);

			Assert.AreEqual(v.GetWireSize(), written);
			AssertHeader(buffer, Vector.ElementType.INT32, data.Length);

			for (int i = 0; i < data.Length; i++)
			{
				Assert.AreEqual(data[i], BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(Vector.HEADER_SIZE + i * 4, 4)));
			}
		}

		[TestMethod]
		public void WriteToFloat16()
		{
			short[] data = new short[] { 0x3c00, unchecked((short)0xbc00), 0x4000 };
			Vector v = Vector.OfFloat16(data);

			byte[] buffer = new byte[v.GetWireSize()];
			int written = v.WriteTo(buffer, 0);

			Assert.AreEqual(v.GetWireSize(), written);
			AssertHeader(buffer, Vector.ElementType.FLOAT16, data.Length);

			for (int i = 0; i < data.Length; i++)
			{
				Assert.AreEqual(data[i], BinaryPrimitives.ReadInt16LittleEndian(buffer.AsSpan(Vector.HEADER_SIZE + i * 2, 2)));
			}
		}

		[TestMethod]
		public void WriteToRespectsOffset()
		{
			Vector v = Vector.OfInt32(new int[] { 7, 8 });

			byte[] buffer = new byte[4 + v.GetWireSize()];
			int written = v.WriteTo(buffer, 4);

			Assert.AreEqual(v.GetWireSize(), written);
			Assert.AreEqual(0, buffer[0]);
			Assert.AreEqual(0, buffer[1]);
			Assert.AreEqual(0, buffer[2]);
			Assert.AreEqual(0, buffer[3]);
			Assert.AreEqual(Vector.VERSION, buffer[4]);
		}

		//-------------------------------------------------------
		// From (deserialization)
		//-------------------------------------------------------

		[TestMethod]
		public void FromRoundTripsFloat16()
		{
			short[] data = new short[] { 0x3c00, unchecked((short)0xbc00), 0x4000 };
			AssertRoundTrip(Vector.OfFloat16(data));
		}

		[TestMethod]
		public void FromRoundTripsInt32()
		{
			AssertRoundTrip(Vector.OfInt32(new int[] { -1, 0, 1, int.MaxValue }));
		}

		[TestMethod]
		public void FromRoundTripsFloat32()
		{
			AssertRoundTrip(Vector.OfFloat32(new float[] { 1.5f, -2.25f, 0.0f, 3.14159f, float.MaxValue }));
		}

		[TestMethod]
		public void FromRoundTripsFloat64()
		{
			AssertRoundTrip(Vector.OfFloat64(new double[] { 1.5, -2.25, double.MaxValue }));
		}

		[TestMethod]
		public void FromRespectsOffset()
		{
			Vector v = Vector.OfInt32(new int[] { 7, 8, 9 });

			byte[] buffer = new byte[4 + v.GetWireSize()];
			v.WriteTo(buffer, 4);

			Vector parsed = Vector.From(buffer, 4, buffer.Length - 4);
			Assert.AreEqual(v, parsed);
		}

		[TestMethod]
		public void FromIgnoresTrailingBytes()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });

			byte[] buffer = new byte[v.GetWireSize() + 10];
			v.WriteTo(buffer, 0);

			Vector parsed = Vector.From(buffer, 0, buffer.Length);
			Assert.AreEqual(v, parsed);
		}

		[TestMethod]
		public void FromRejectsShortBuffer()
		{
			Assert.Throws<ArgumentException>(() => Vector.From(new byte[4], 0, 4));
		}

		[TestMethod]
		public void FromTooShortThrows()
		{
			Assert.Throws<ArgumentException>(() => Vector.From(new byte[Vector.HEADER_SIZE - 1], 0, Vector.HEADER_SIZE - 1));
		}

		[TestMethod]
		public void FromTruncatedDataThrows()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);
			Assert.Throws<ArgumentException>(() => Vector.From(buffer, 0, buffer.Length - 1));
		}

		[TestMethod]
		public void FromInvalidElementTypeThrows()
		{
			byte[] buffer = new byte[Vector.HEADER_SIZE];
			buffer[0] = Vector.VERSION;
			buffer[1] = 0x7f;
			Assert.Throws<ArgumentException>(() => Vector.From(buffer, 0, buffer.Length));
		}

		//-------------------------------------------------------
		// Value / Bin / particle plumbing
		//-------------------------------------------------------

		[TestMethod]
		public void BytesToParticleDeserializesVector()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.25f, 3.0f });
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);

			object parsed = ByteUtil.BytesToParticle(ParticleType.VECTOR, buffer, 0, buffer.Length);
			Assert.IsInstanceOfType(parsed, typeof(Vector));
			Assert.AreEqual(v, parsed);
		}

		[TestMethod]
		public void UnpackerDeserializesNestedVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Packer packer = new Packer();
			packer.PackList(new List<object> { Value.Get(v) });
			byte[] packed = packer.ToByteArray();

			Unpacker unpacker = new Unpacker(packed, 0, packed.Length, false);
			IList unpackedList = (IList)unpacker.UnpackObject();
			Assert.AreEqual(1, unpackedList.Count);
			Assert.IsInstanceOfType(unpackedList[0], typeof(Vector));
			Assert.AreEqual(v, unpackedList[0]);
		}

		[TestMethod]
		public void UnpackerRoundTripsRawVectorInList()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.25f, 3.0f });
			byte[] packed = Packer.Pack(new List<object> { v });

			Unpacker unpacker = new Unpacker(packed, 0, packed.Length, false);
			IList unpackedList = (IList)unpacker.UnpackObject();
			Assert.AreEqual(1, unpackedList.Count);
			Assert.IsInstanceOfType(unpackedList[0], typeof(Vector));
			Assert.AreEqual(v, unpackedList[0]);
		}

		[TestMethod]
		public void BinWithRawVectorInListPacksSuccessfully()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Bin bin = new Bin("veclist", new List<object> { v });

			byte[] buffer = new byte[bin.value.EstimateSize()];
			int written = bin.value.Write(buffer, 0);
			Assert.AreEqual(buffer.Length, written);
		}

		[TestMethod]
		public void GetVectorValue()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f });
			Value value = Value.Get(v);

			Assert.AreEqual(ParticleType.VECTOR, value.Type);
			Assert.AreSame(v, value.Object);
			Assert.AreSame(v, ((Value.VectorValue)value).Vector);
		}

		[TestMethod]
		public void GetVectorNull()
		{
			Assert.AreSame(Value.AsNull, Value.Get((Vector)null));
		}

		[TestMethod]
		public void GetObjectWrapsNativeVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Value value = Value.Get((object)v);

			Assert.IsInstanceOfType(value, typeof(Value.VectorValue));
			Assert.AreEqual(ParticleType.VECTOR, value.Type);
			Assert.AreSame(v, ((Value.VectorValue)value).Vector);
		}

		[TestMethod]
		public void ValueEstimateSizeMatchesWireSize()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
			Assert.AreEqual(v.GetWireSize(), Value.Get(v).EstimateSize());
		}

		[TestMethod]
		public void ValueWriteMatchesVectorWriteTo()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.25f, 3.0f });
			Value value = Value.Get(v);

			byte[] expected = new byte[v.GetWireSize()];
			v.WriteTo(expected, 0);

			byte[] actual = new byte[value.EstimateSize()];
			int written = value.Write(actual, 0);

			Assert.AreEqual(expected.Length, written);
			CollectionAssert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void ValueEquals()
		{
			Value a = Value.Get(Vector.OfInt32(new int[] { 1, 2, 3 }));
			Value b = Value.Get(Vector.OfInt32(new int[] { 1, 2, 3 }));

			Assert.AreEqual(a, b);
			Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
		}

		[TestMethod]
		public void ValidateKeyTypeThrows()
		{
			Value value = Value.Get(Vector.OfInt32(new int[] { 1, 2, 3 }));
			AerospikeException e = Assert.Throws<AerospikeException>(() => value.ValidateKeyType());
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
		}

		[TestMethod]
		public void PackProducesParticleBytes()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Value value = Value.Get(v);

			Packer packer = new Packer();
			value.Pack(packer);
			byte[] packed = packer.ToByteArray();

			byte[] wire = v.GetWireBytes();
			int payloadStart = packed.Length - wire.Length;
			Assert.AreEqual((byte)ParticleType.VECTOR, packed[payloadStart - 1]);
			CollectionAssert.AreEqual(wire, packed.AsSpan(payloadStart, wire.Length).ToArray());
		}

		[TestMethod]
		public void BinConstructorWrapsVector()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f, 3.0f });
			Bin bin = new Bin("vecbin", v);

			Assert.AreEqual("vecbin", bin.name);
			Assert.IsInstanceOfType(bin.value, typeof(Value.VectorValue));
			Assert.AreEqual(ParticleType.VECTOR, bin.value.Type);
			Assert.AreSame(v, ((Value.VectorValue)bin.value).Vector);
		}

		[TestMethod]
		public void BinEqualsUsesVectorEquality()
		{
			Bin a = new Bin("vecbin", Vector.OfInt32(new int[] { 1, 2, 3 }));
			Bin b = new Bin("vecbin", Vector.OfInt32(new int[] { 1, 2, 3 }));

			Assert.AreEqual(a, b);
			Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
		}

		[TestMethod]
		public void ValueAndBinWireParticleType()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f, 2.0f });
			Value value = Value.Get(v);

			Assert.AreEqual(ParticleType.VECTOR, value.Type);
			Assert.IsInstanceOfType(value, typeof(Value.VectorValue));

			Bin bin = new Bin("emb", v);
			Assert.AreEqual(ParticleType.VECTOR, bin.value.Type);

			byte[] wire = new byte[value.EstimateSize()];
			Assert.AreEqual(v.GetWireSize(), value.Write(wire, 0));
			Assert.AreEqual(v, Vector.From(wire, 0, wire.Length));
		}

		[TestMethod]
		public void BytesToParticleRoundTrip()
		{
			Vector v = Vector.OfInt32(new int[] { 9, 8, 7 });
			byte[] wire = v.GetWireBytes();

			object parsed = ByteUtil.BytesToParticle(ParticleType.VECTOR, wire, 0, wire.Length);
			Assert.AreEqual(v, parsed);
		}

		[TestMethod]
		public void PackUnpackRoundTrip()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.5f, -2.25f });
			Packer packer = new Packer();
			packer.PackVector(v);
			byte[] packed = packer.ToByteArray();

			Unpacker unpacker = new Unpacker(packed, 0, packed.Length, false);
			object unpacked = unpacker.UnpackObject();
			Assert.AreEqual(v, unpacked);
		}

		[TestMethod]
		public void RecordGetVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2 });
			Record record = new Record(new Dictionary<string, object> { { "emb", v } }, 1, 0);
			Assert.AreSame(v, record.GetVector("emb"));
			Assert.IsNull(record.GetVector("missing"));
		}

		[TestMethod]
		public void VectorExpDistancePacks()
		{
			Vector query = Vector.OfFloat32(new float[] { 1.0f, 0.0f });
			Exp exp = VectorExp.Distance(VectorDistanceMetric.COSINE, query, Exp.VectorBin("emb"));

			Packer packer = new Packer();
			exp.Pack(packer);

			byte[] packed = packer.ToByteArray();
			byte[] wire = query.GetWireBytes();

			// VectorExp packs the query as particle bytes (BLOB + wire payload) plus the
			// cosine-distance opcode. Asserting the public byte stream avoids relying on
			// Packer.HasVector(), which is internal-only.
			Assert.IsTrue(packed.Length > wire.Length);
			Assert.IsTrue(packed.AsSpan().IndexOf(wire) >= 0, "Packed expression should embed query vector wire bytes");
			Assert.IsTrue(packed.AsSpan().IndexOf((byte)54) >= 0, "Packed expression should include cosine distance opcode");
		}

		//-------------------------------------------------------
		// hasVector tracking (fail-fast)
		//-------------------------------------------------------

		[TestMethod]
		public void VectorValueReportsHasVector()
		{
			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			Assert.IsTrue(Internals.HasVector(Value.Get(v)));
			Assert.IsFalse(Internals.HasVector(Value.Get(1)));
			Assert.IsFalse(Internals.HasVector(Value.Get(new byte[] { 1, 2 })));
		}

		[TestMethod]
		public void BytesValueVectorFlag()
		{
			byte[] bytes = new byte[] { 1, 2, 3 };
			Assert.IsFalse(Internals.HasVector(Value.Get(bytes)));
			Assert.IsTrue(Internals.HasVector(Value.Get(bytes, true)));
		}

		[TestMethod]
		public void ListAndMapDetectNestedVector()
		{
			Vector v = Vector.OfFloat32(new float[] { 1.0f });
			Value.ListValue list = new(new List<object> { "a", v });
			Assert.IsTrue(Internals.HasVector(list));

			Value.MapValue map = new(new Dictionary<object, object> { { "emb", v } });
			Assert.IsTrue(Internals.HasVector(map));

			Value.ValueArray arr = new(new Value[] { Value.Get("x"), Value.Get(v) });
			Assert.IsTrue(Internals.HasVector(arr));
		}

		[TestMethod]
		public void ExpressionFromVectorExpReportsHasVector()
		{
			Vector query = Vector.OfFloat32(new float[] { 1.0f, 0.0f });
			Expression exp = Exp.Build(VectorExp.Distance(VectorDistanceMetric.COSINE, query, Exp.VectorBin("emb")));
			Assert.IsTrue(Internals.HasVector(exp));

			Expression plain = Exp.Build(Exp.EQ(Exp.IntBin("a"), Exp.Val(1)));
			Assert.IsFalse(Internals.HasVector(plain));
		}

		private static void AssertRoundTrip(Vector v)
		{
			byte[] buffer = new byte[v.GetWireSize()];
			v.WriteTo(buffer, 0);
			Assert.AreEqual(v, Vector.From(buffer, 0, buffer.Length));
		}

		private static void AssertHeader(byte[] buffer, Vector.ElementType elementType, int dimensions)
		{
			Assert.AreEqual(Vector.VERSION, buffer[0]);
			Assert.AreEqual((byte)elementType, buffer[1]);
			Assert.AreEqual(dimensions, ByteUtil.LittleBytesToInt(buffer, 2));
		}
	}
}
