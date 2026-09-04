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
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestValue
	{
		private enum IntEnum
		{
			Value = 123
		}

		private enum LongEnum : long
		{
			Value = 1234567890123L
		}

		private enum UIntEnum : uint
		{
			Value = 4000000000U
		}

		private enum ULongEnum : ulong
		{
			Value = 9223372036854775813UL
		}

		private enum ShortEnum : short
		{
			Value = -123
		}

		private enum UShortEnum : ushort
		{
			Value = 123
		}

		private enum ByteEnum : byte
		{
			Value = 123
		}

		private enum SByteEnum : sbyte
		{
			Value = -123
		}

		[TestMethod]
		public void EnumValuesUseUnderlyingIntegerType()
		{
			AssertValue<IntEnum, Value.IntegerValue, int>(IntEnum.Value, 123);
			AssertValue<LongEnum, Value.LongValue, long>(LongEnum.Value, 1234567890123L);
			AssertValue<UIntEnum, Value.UnsignedIntegerValue, uint>(UIntEnum.Value, 4000000000U);
			AssertValue<ULongEnum, Value.UnsignedLongValue, ulong>(ULongEnum.Value, 9223372036854775813UL);
			AssertValue<ShortEnum, Value.ShortValue, short>(ShortEnum.Value, -123);
			AssertValue<UShortEnum, Value.UnsignedShortValue, ushort>(UShortEnum.Value, 123);
			AssertValue<ByteEnum, Value.ByteValue, byte>(ByteEnum.Value, 123);
			AssertValue<SByteEnum, Value.SignedByteValue, sbyte>(SByteEnum.Value, -123);
		}

		[TestMethod]
		public void ListValuesCompareByContents()
		{
			Value left = Value.Get((IList)new string[] { "123", "456" });
			Value right = Value.Get((IList)new string[] { "123", "456" });

			Assert.AreEqual(left, right);
		}

		[TestMethod]
		public void MapValuesCompareByContents()
		{
			Hashtable leftMap = new()
			{
				["one"] = 1,
				["two"] = "second"
			};
			Hashtable rightMap = new()
			{
				["one"] = 1,
				["two"] = "second"
			};

			Value left = Value.Get((IDictionary)leftMap);
			Value right = Value.Get((IDictionary)rightMap);

			Assert.AreEqual(left, right);
		}

		[TestMethod]
		public void ValueArraysCompareByContents()
		{
			Value left = Value.Get(new Value[] { Value.Get("123"), Value.Get(456) });
			Value right = Value.Get(new Value[] { Value.Get("123"), Value.Get(456) });

			Assert.AreEqual(left, right);
		}

		[TestMethod]
		public void BlobValueGetReturnsExistingValue()
		{
			Value.BlobValue blob = new(new byte[] { 1, 2, 3 });

			Assert.AreSame(blob, Value.Get(blob));
		}

		[TestMethod]
		public void BoolIntValuesCompareByContents()
		{
			Assert.AreEqual(new Value.BoolIntValue(true), new Value.BoolIntValue(true));
			Assert.AreNotEqual(new Value.BoolIntValue(true), new Value.BoolIntValue(false));
		}

		[TestMethod]
		public void BytesValueComparesToByteArray()
		{
			Value.BytesValue bytes = new(new byte[] { 1, 2, 3 });

			Assert.IsTrue(bytes == new byte[] { 1, 2, 3 });
			Assert.IsTrue(bytes != new byte[] { 1, 2, 4 });
		}

		[TestMethod]
		public void FloatAndDoubleValuesRoundTrip()
		{
			Value floatValue = Value.Get(3.5f);
			Value doubleValue = Value.Get(2.25d);

			Assert.IsInstanceOfType(floatValue, typeof(Value.FloatValue));
			Assert.IsInstanceOfType(doubleValue, typeof(Value.DoubleValue));
			Assert.AreEqual(3.5f, (float)floatValue.Object);
			Assert.AreEqual(2.25d, (double)doubleValue.Object);
			Assert.AreEqual(floatValue, Value.Get(3.5f));
			Assert.AreEqual(doubleValue, Value.Get(2.25d));
		}

		[TestMethod]
		public void GeoJsonValueRoundTrip()
		{
			string json = "{ \"type\": \"Point\", \"coordinates\": [0.0, 0.0] }";
			Value geo = Value.GetAsGeoJSON(json);

			Assert.IsInstanceOfType(geo, typeof(Value.GeoJSONValue));
			Assert.AreEqual(json, geo.Object);
			Assert.AreEqual(geo, Value.GetAsGeoJSON(json));
		}

		[TestMethod]
		public void NullAndWildcardValues()
		{
			Assert.IsTrue(Value.AsNull.IsNull);
			Assert.AreEqual(Value.AsNull, Value.Get((string)null));
			Assert.AreSame(Value.WILDCARD, Value.WILDCARD);
			Assert.AreSame(Value.INFINITY, Value.INFINITY);
		}

		[TestMethod]
		public void ByteSegmentValueComparesSliceNotWholeArray()
		{
			byte[] backing = [1, 2, 3, 4, 5];
			Value.ByteSegmentValue left = new(backing, 1, 3);
			Value.ByteSegmentValue right = new(backing, 1, 3);
			Value.ByteSegmentValue different = new(backing, 2, 3);

			Assert.AreEqual(left, right);
			Assert.AreNotEqual(left, different);
			Assert.AreEqual(left.GetHashCode(), right.GetHashCode());
			Assert.AreEqual("020304", left.ToString());
			Assert.AreEqual(ParticleType.BLOB, left.Type);
			Assert.AreEqual(3, left.EstimateSize());

			byte[] buffer = new byte[3];
			Assert.AreEqual(3, left.Write(buffer, 0));
			CollectionAssert.AreEqual(new byte[] { 2, 3, 4 }, buffer);
		}

		[TestMethod]
		public void ByteSegmentValueRoundTripsThroughValueGet()
		{
			byte[] backing = [9, 8, 7, 6];
			Value value = Value.Get(backing, 1, 2);

			Assert.IsInstanceOfType(value, typeof(Value.ByteSegmentValue));
			Value.ByteSegmentValue segment = (Value.ByteSegmentValue)value;
			Assert.AreEqual(1, segment.Offset);
			Assert.AreEqual(2, segment.Length);
		}

		[TestMethod]
		public void ReadOnlyMemoryBytesValueComparesContents()
		{
			ReadOnlyMemory<byte> bytes = new byte[] { 10, 20, 30 };
			Value.ReadOnlyMemoryBytesValue left = new(bytes);
			Value.ReadOnlyMemoryBytesValue right = new(bytes);
			Value.ReadOnlyMemoryBytesValue different = new(new byte[] { 10, 20, 31 });

			Assert.AreEqual(left, right);
			Assert.IsTrue(left.Equals(bytes));
			Assert.IsFalse(left.Equals(different.Bytes));
			Assert.AreEqual(left.GetHashCode(), right.GetHashCode());
			Assert.AreEqual("0A141E", left.ToString());
			Assert.AreEqual(ParticleType.BLOB, left.Type);

			byte[] buffer = new byte[3];
			Assert.AreEqual(3, left.Write(buffer, 0));
			CollectionAssert.AreEqual(bytes.ToArray(), buffer);
		}

		[TestMethod]
		public void ReadOnlyMemoryBytesValueRoundTripsThroughValueGet()
		{
			ReadOnlyMemory<byte> bytes = new byte[] { 1, 2 };
			Value value = Value.Get(bytes);

			Assert.IsInstanceOfType(value, typeof(Value.ReadOnlyMemoryBytesValue));
			CollectionAssert.AreEqual(bytes.ToArray(), ((Value.ReadOnlyMemoryBytesValue)value).Bytes.ToArray());
		}

		[TestMethod]
		public void HLLValueComparesBytesAndRejectsKeys()
		{
			byte[] bytes = [0x01, 0x02, 0x03];
			Value.HLLValue left = new(bytes);
			Value.HLLValue right = new((byte[])bytes.Clone());
			byte[] different = [0x01, 0x02, 0x04];

			Assert.AreEqual(left, right);
			Assert.IsTrue(left.Equals(bytes));
			Assert.IsFalse(left.Equals(different));
			Assert.AreEqual(left.GetHashCode(), right.GetHashCode());
			Assert.AreEqual(ParticleType.HLL, left.Type);
			Assert.AreEqual("010203", left.ToString());

			Test.TestException(() => left.ValidateKeyType(), ResultCode.PARAMETER_ERROR);
		}

		private static void AssertValue<TEnum, TValue, TObject>(TEnum enumValue, TObject expected)
			where TEnum : struct, Enum
			where TValue : Value
		{
			Value value = Value.Get(enumValue);

			Assert.IsInstanceOfType(value, typeof(TValue));
			Assert.AreEqual(expected, value.Object);
		}
	}
}
