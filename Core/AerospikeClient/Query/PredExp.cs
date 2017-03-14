/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Predicate expression filter.
	/// Predicate expression filters are applied on the query results on the server.
	/// Predicate expression filters may occur on any bin in the record.
	/// </summary>
	public abstract class PredExp
	{
		/// <summary>
		/// Create "and" expression.
		/// </summary>
		/// <param name="nexp">	number of expressions to perform "and" operation.  Usually two. </param>
		public static PredExp And(ushort nexp)
		{
			return new AndOr(AND, nexp);
		}

		/// <summary>
		/// Create "or" expression.
		/// </summary>
		/// <param name="nexp">	number of expressions to perform "or" operation.  Usually two. </param>
		public static PredExp Or(ushort nexp)
		{
			return new AndOr(OR, nexp);
		}

		/// <summary>
		/// Create "not" expression.
		/// </summary>
		public static PredExp Not()
		{
			return new Op(NOT);
		}

		/// <summary>
		/// Create Calendar value expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
		/// </summary>
		public static PredExp IntegerValue(DateTime val)
		{
			return new IntegerVal(Util.NanosFromEpoch(val), INTEGER_VALUE);
		}

		/// <summary>
		/// Create 64 bit integer value.
		/// </summary>
		public static PredExp IntegerValue(long val)
		{
			return new IntegerVal(val, INTEGER_VALUE);
		}

		/// <summary>
		/// Create string value.
		/// </summary>
		public static PredExp StringValue(string val)
		{
			return new StringVal(val, STRING_VALUE);
		}

		/// <summary>
		/// Create geospatial json string value.
		/// </summary>
		public static PredExp GeoJSONValue(string val)
		{
			return new StringVal(val, GEOJSON_VALUE);
		}

		/// <summary>
		/// Create 64 bit integer bin predicate.
		/// </summary>
		public static PredExp IntegerBin(string name)
		{
			return new StringVal(name, INTEGER_BIN);
		}

		/// <summary>
		/// Create string bin predicate.
		/// </summary>
		public static PredExp StringBin(string name)
		{
			return new StringVal(name, STRING_BIN);
		}

		/// <summary>
		/// Create geospatial bin predicate.
		/// </summary>
		public static PredExp GeoJSONBin(string name)
		{
			return new StringVal(name, GEOJSON_BIN);
		}

		/// <summary>
		/// Create list bin predicate.
		/// </summary>
		public static PredExp ListBin(string name)
		{
			return new StringVal(name, LIST_BIN);
		}

		/// <summary>
		/// Create map bin predicate.
		/// </summary>
		public static PredExp MapBin(string name)
		{
			return new StringVal(name, MAP_BIN);
		}

		/// <summary>
		/// Create 64 bit integer variable used in list/map iterations.
		/// </summary>
		public static PredExp IntegerVar(string name)
		{
			return new StringVal(name, INTEGER_VAR);
		}

		/// <summary>
		/// Create string variable used in list/map iterations.
		/// </summary>
		public static PredExp StringVar(string name)
		{
			return new StringVal(name, STRING_VAR);
		}

		/// <summary>
		/// Create geospatial json string variable used in list/map iterations.
		/// </summary>
		public static PredExp GeoJSONVar(string name)
		{
			return new StringVal(name, GEOJSON_VAR);
		}

		/// <summary>
		/// Create record size on disk predicate.
		/// </summary>
		public static PredExp RecDeviceSize()
		{
			return new Op(RECSIZE);
		}

		/// <summary>
		/// Create record last update time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
		/// Example:
		/// <pre>
		/// // Record last update time >= 2017-01-15
		/// PredExp.RecLastUpdate()
		/// PredExp.IntegerValue(new DateTime(2017, 1, 15))
		/// PredExp.IntegerGreaterEq()
		/// </pre>
		/// </summary>
		public static PredExp RecLastUpdate()
		{
			return new Op(LAST_UPDATE);
		}

		/// <summary>
		/// Create record expiration time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
		/// Example:
		/// <pre>
		/// // Record expires on 2020-01-01
		/// PredExp.RecVoidTime()
		/// PredExp.IntegerValue(new DateTime(2020, 0, 1))
		/// PredExp.IntegerGreaterEq()
		/// PredExp.RecVoidTime()
		/// PredExp.IntegerValue(new DateTime(2020, 0, 2))
		/// PredExp.IntegerLess()
		/// PredExp.And(2)
		/// </pre>
		/// </summary>
		public static PredExp RecVoidTime()
		{
			return new Op(VOID_TIME);
		}

		/// <summary>
		/// Create 64 bit integer "=" operation predicate.
		/// </summary>
		public static PredExp IntegerEqual()
		{
			return new Op(INTEGER_EQUAL);
		}

		/// <summary>
		/// Create 64 bit integer "!=" operation predicate.
		/// </summary>
		public static PredExp IntegerUnequal()
		{
			return new Op(INTEGER_UNEQUAL);
		}

		/// <summary>
		/// Create 64 bit integer ">" operation predicate.
		/// </summary>
		public static PredExp IntegerGreater()
		{
			return new Op(INTEGER_GREATER);
		}

		/// <summary>
		/// Create 64 bit integer ">=" operation predicate.
		/// </summary>
		public static PredExp IntegerGreaterEq()
		{
			return new Op(INTEGER_GREATEREQ);
		}

		/// <summary>
		/// Create 64 bit integer "&lt;" operation predicate.
		/// </summary>
		public static PredExp IntegerLess()
		{
			return new Op(INTEGER_LESS);
		}

		/// <summary>
		/// Create 64 bit integer "&lt;=" operation predicate.
		/// </summary>
		public static PredExp IntegerLessEq()
		{
			return new Op(INTEGER_LESSEQ);
		}

		/// <summary>
		/// Create string "=" operation predicate.
		/// </summary>
		public static PredExp StringEqual()
		{
			return new Op(STRING_EQUAL);
		}

		/// <summary>
		/// Create string "!=" operation predicate.
		/// </summary>
		public static PredExp StringUnequal()
		{
			return new Op(STRING_UNEQUAL);
		}

		/// <summary>
		/// Create regular expression string operation predicate.  Example:
		/// <pre>
		/// PredExp.StringRegex(RegexFlag.EXTENDED | RegexFlag.ICASE)
		/// </pre>
		/// </summary>
		/// <param name="flags">regular expression bit flags. See <see cref="RegexFlag"/></param>
		public static PredExp StringRegex(uint flags)
		{
			return new Regex(STRING_REGEX, flags);
		}

		/// <summary>
		/// Create geospatial json "within" predicate.
		/// </summary>
		public static PredExp GeoJSONWithin()
		{
			return new Op(GEOJSON_WITHIN);
		}

		/// <summary>
		/// Create geospatial json "contains" predicate.
		/// </summary>
		public static PredExp GeoJSONContains()
		{
			return new Op(GEOJSON_CONTAINS);
		}

		/// <summary>
		/// Create list predicate where expression matches for any list item.
		/// Example:
		/// <pre>
		/// // Find records where any list item v = "hello" in list bin x.  
		/// PredExp.StringVar("v")
		/// PredExp.StringValue("hello")
		/// PredExp.StringEqual()
		/// PredExp.ListBin("x")
		/// PredExp.ListIterateOr("v")
		/// </pre>
		/// </summary>
		public static PredExp ListIterateOr(string varName)
		{
			return new StringVal(varName, LIST_ITERATE_OR);
		}

		/// <summary>
		/// Create list predicate where expression matches for all list items.
		/// Example:
		/// <pre>
		/// // Find records where all list elements v != "goodbye" in list bin x.  
		/// PredExp.StringVar("v")
		/// PredExp.StringValue("goodbye")
		/// PredExp.StringUnequal()
		/// PredExp.ListBin("x")
		/// PredExp.ListIterateAnd("v")
		/// </pre>
		/// </summary>
		public static PredExp ListIterateAnd(string varName)
		{
			return new StringVal(varName, LIST_ITERATE_AND);
		}

		/// <summary>
		/// Create map predicate where expression matches for any map key.
		/// Example:
		/// <pre>
		/// // Find records where any map key k = 7 in map bin m.  
		/// PredExp.IntegerVar("k")
		/// PredExp.IntegerValue(7)
		/// PredExp.IntegerEqual()
		/// PredExp.MapBin("m")
		/// PredExp.MapKeyIterateOr("k")
		/// </pre>
		/// </summary>
		public static PredExp MapKeyIterateOr(string varName)
		{
			return new StringVal(varName, MAPKEY_ITERATE_OR);
		}

		/// <summary>
		/// Create map key predicate where expression matches for all map keys.
		/// Example:
		/// <pre>
		/// // Find records where all map keys k &lt; 5 in map bin m.  
		/// PredExp.IntegerVar("k")
		/// PredExp.IntegerValue(5)
		/// PredExp.IntegerLess()
		/// PredExp.MapBin("m")
		/// PredExp.MapKeyIterateAnd("k")
		/// </pre>
		/// </summary>
		public static PredExp MapKeyIterateAnd(string varName)
		{
			return new StringVal(varName, MAPKEY_ITERATE_AND);
		}

		/// <summary>
		/// Create map predicate where expression matches for any map value.
		/// <pre>
		/// // Find records where any map value v > 100 in map bin m.  
		/// PredExp.IntegerVar("v")
		/// PredExp.IntegerValue(100)
		/// PredExp.IntegerGreater()
		/// PredExp.MapBin("m")
		/// PredExp.MapValIterateOr("v")
		/// </pre>
		/// </summary>
		public static PredExp MapValIterateOr(string varName)
		{
			return new StringVal(varName, MAPVAL_ITERATE_OR);
		}

		/// <summary>
		/// Create map predicate where expression matches for all map values.
		/// Example:
		/// <pre>
		/// // Find records where all map values v > 500 in map bin m.  
		/// PredExp.IntegerVar("v")
		/// PredExp.IntegerValue(500)
		/// PredExp.IntegerGreater()
		/// PredExp.MapBin("m")
		/// PredExp.MapKeyIterateAnd("v")
		/// </pre>
		/// </summary>
		public static PredExp MapValIterateAnd(string varName)
		{
			return new StringVal(varName, MAPVAL_ITERATE_AND);
		}

		private const ushort AND = 1;
		private const ushort OR = 2;
		private const ushort NOT = 3;
		private const ushort INTEGER_VALUE = 10;
		private const ushort STRING_VALUE = 11;
		private const ushort GEOJSON_VALUE = 12;
		private const ushort INTEGER_BIN = 100;
		private const ushort STRING_BIN = 101;
		private const ushort GEOJSON_BIN = 102;
		private const ushort LIST_BIN = 103;
		private const ushort MAP_BIN = 104;
		private const ushort INTEGER_VAR = 120;
		private const ushort STRING_VAR = 121;
		private const ushort GEOJSON_VAR = 122;
		private const ushort RECSIZE = 150;
		private const ushort LAST_UPDATE = 151;
		private const ushort VOID_TIME = 152;
		private const ushort INTEGER_EQUAL = 200;
		private const ushort INTEGER_UNEQUAL = 201;
		private const ushort INTEGER_GREATER = 202;
		private const ushort INTEGER_GREATEREQ = 203;
		private const ushort INTEGER_LESS = 204;
		private const ushort INTEGER_LESSEQ = 205;
		private const ushort STRING_EQUAL = 210;
		private const ushort STRING_UNEQUAL = 211;
		private const ushort STRING_REGEX = 212;
		private const ushort GEOJSON_WITHIN = 220;
		private const ushort GEOJSON_CONTAINS = 221;
		private const ushort LIST_ITERATE_OR = 250;
		private const ushort MAPKEY_ITERATE_OR = 251;
		private const ushort MAPVAL_ITERATE_OR = 252;
		private const ushort LIST_ITERATE_AND = 253;
		private const ushort MAPKEY_ITERATE_AND = 254;
		private const ushort MAPVAL_ITERATE_AND = 255;

		private const long NANOS_PER_MILLIS = 1000000L;

		/// <summary>
		/// Estimate size of predicate expressions.
		/// For internal use only.
		/// </summary>
		public static int EstimateSize(PredExp[] predExp)
		{
			int size = 0;

			foreach (PredExp pred in predExp)
			{
				size += pred.EstimateSize();
			}
			return size;
		}

		/// <summary>
		/// Write predicate expressions to write protocol.
		/// For internal use only.
		/// </summary>
		public static int Write(PredExp[] predExp, byte[] buf, int offset)
		{
			foreach (PredExp pred in predExp)
			{
				offset = pred.Write(buf, offset);
			}
			return offset;
		}

		/// <summary>
		/// Estimate size of predicate expression.
		/// For internal use only.
		/// </summary>
		public abstract int EstimateSize();

		/// <summary>
		/// Write predicate expression to write protocol.
		/// For internal use only.
		/// </summary>
		public abstract int Write(byte[] buf, int offset);

		private class IntegerVal : PredExp
		{
			internal readonly long value;
			internal readonly ushort type;

			internal IntegerVal(long value, ushort type)
			{
				this.value = value;
				this.type = type;
			}

			public override int EstimateSize()
			{
				return 14;
			}

			public override int Write(byte[] buf, int offset)
			{
				// Write value type
				ByteUtil.ShortToBytes(type, buf, offset);
				offset += 2;

				// Write length
				ByteUtil.IntToBytes(8, buf, offset);
				offset += 4;

				// Write value
				ByteUtil.LongToBytes((ulong)value, buf, offset);
				offset += 8;
				return offset;
			}
		}

		private class StringVal : PredExp
		{
			internal readonly string value;
			internal readonly ushort type;

			public StringVal(string value, ushort type)
			{
				this.value = value;
				this.type = type;
			}

			public override int EstimateSize()
			{
				return ByteUtil.EstimateSizeUtf8(value) + 6;
			}

			public override int Write(byte[] buf, int offset)
			{
				// Write value type
				ByteUtil.ShortToBytes(type, buf, offset);
				offset += 2;

				// Write value
				int len = ByteUtil.StringToUtf8(value, buf, offset + 4);
				ByteUtil.IntToBytes((uint)len, buf, offset);
				offset += 4 + len;
				return offset;
			}
		}

		private class AndOr : PredExp
		{
			internal readonly ushort op;
			internal readonly ushort nexp;

			internal AndOr(ushort op, ushort nexp)
			{
				this.op = op;
				this.nexp = nexp;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buf, int offset)
			{
				// Write type
				ByteUtil.ShortToBytes(op, buf, offset);
				offset += 2;

				// Write length
				ByteUtil.IntToBytes(2, buf, offset);
				offset += 4;

				// Write predicate count
				ByteUtil.ShortToBytes(nexp, buf, offset);
				offset += 2;
				return offset;
			}
		}

		private class Op : PredExp
		{
			internal readonly ushort op;

			internal Op(ushort op)
			{
				this.op = op;
			}

			public override int EstimateSize()
			{
				return 6;
			}

			public override int Write(byte[] buf, int offset)
			{
				// Write op type
				ByteUtil.ShortToBytes(op, buf, offset);
				offset += 2;

				// Write zero length
				ByteUtil.IntToBytes(0, buf, offset);
				offset += 4;
				return offset;
			}
		}

		private class Regex : PredExp
		{
			internal readonly uint flags;
			internal readonly ushort op;

			internal Regex(ushort op, uint flags)
			{
				this.op = op;
				this.flags = flags;
			}

			public override int EstimateSize()
			{
				return 10;
			}

			public override int Write(byte[] buf, int offset)
			{
				// Write op type
				ByteUtil.ShortToBytes(op, buf, offset);
				offset += 2;

				// Write length
				ByteUtil.IntToBytes(4, buf, offset);
				offset += 4;

				// Write predicate count
				ByteUtil.IntToBytes(flags, buf, offset);
				offset += 4;
				return offset;
			}
		}
	}
}
