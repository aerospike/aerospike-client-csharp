/*
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Expression generator.
	/// </summary>
	public abstract class Exp
	{
		/// <summary>
		/// Expression type.
		/// </summary>
		public enum Type
		{
			NIL = 0,
			BOOL = 1,
			INT = 2,
			STRING = 3,
			LIST = 4,
			MAP = 5,
			BLOB = 6,
			FLOAT = 7,
			GEO = 8,
			HLL = 9
		}

		//--------------------------------------------------
		// Build
		//--------------------------------------------------

		/// <summary>
		/// Create final expression that contains packed byte instructions used in the wire protocol.
		/// </summary>
		public static Expression Build(Exp exp)
		{
			return new Expression(exp);
		}

		//--------------------------------------------------
		// Record Key
		//--------------------------------------------------

		/// <summary>
		/// Create record key expression of specified type.
		/// </summary>
		/// <example>
		/// <code>
		/// // Integer record key >= 100000
		/// Exp.GE(Exp.Key(Type.INT), Exp.Val(100000))
		/// </code>
		/// </example>
		public static Exp Key(Type type)
		{
			return new CmdInt(KEY, (int)type);
		}

		/// <summary>
		/// Create expression that returns if the primary key is stored in the record meta data
		/// as a boolean expression. This would occur when <see cref="Aerospike.Client.Policy.sendKey"/>
		/// is true on record write. This expression usually evaluates quickly because record meta data is
		/// cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Key exists in record meta data
		/// Exp.KeyExists()
		/// </code>
		/// </example>
		public static Exp KeyExists()
		{
			return new Cmd(KEY_EXISTS);
		}

		//--------------------------------------------------
		// Record Bin
		//--------------------------------------------------

		/// <summary>
		/// Create bin expression of specified type.
		/// </summary>
		/// <example>
		/// <code>
		/// // String bin "a" == "views"
		/// Exp.EQ(Exp.Bin("a", Type.STRING), Exp.Val("views"))
		/// </code>
		/// </example>
		public static Exp Bin(string name, Type type)
		{
			return new BinExp(name, type);
		}

		/// <summary>
		/// Create 64 bit integer bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Integer bin "a" == 200
		/// Exp.EQ(Exp.IntBin("a"), Exp.Val(200))
		/// </code>
		/// </example>
		public static Exp IntBin(string name)
		{
			return new BinExp(name, Type.INT);
		}

		/// <summary>
		/// Create 64 bit float bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Float bin "a" >= 1.5
		/// Exp.GE(Exp.FloatBin("a"), Exp.Val(1.5))
		/// </code>
		/// </example>
		public static Exp FloatBin(string name)
		{
			return new BinExp(name, Type.FLOAT);
		}

		/// <summary>
		/// Create string bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // String bin "a" == "views"
		/// Exp.EQ(Exp.StringBin("a"), Exp.Val("views"))
		/// </code>
		/// </example>
		public static Exp StringBin(string name)
		{
			return new BinExp(name, Type.STRING);
		}

		/// <summary>
		/// Create boolean bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Boolean bin "a" == true
		/// Exp.EQ(Exp.BoolBin("a"), Exp.Val(true))
		/// </code>
		/// </example>
		public static Exp BoolBin(string name)
		{
			return new BinExp(name, Type.BOOL);
		}

		/// <summary>
		/// Create byte[] bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Blob bin "a" == [1,2,3]
		/// Exp.EQ(Exp.BlobBin("a"), Exp.Val(new byte[] {1, 2, 3}))
		/// </code>
		/// </example>
		public static Exp BlobBin(string name)
		{
			return new BinExp(name, Type.BLOB);
		}

		/// <summary>
		/// Create geospatial bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Geo bin "a" == region
		/// string region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
		/// Exp.GeoCompare(Exp.GeoBin("loc"), Exp.Geo(region))
		/// </code>
		/// </example>
		public static Exp GeoBin(string name)
		{
			return new BinExp(name, Type.GEO);
		}

		/// <summary>
		/// Create list bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin a[2] == 3
		/// Exp.EQ(ListExp.GetByIndex(ListReturnType.VALUE, Type.INT, Exp.Val(2), Exp.ListBin("a")), Exp.Val(3))
		/// </code>
		/// </example>
		public static Exp ListBin(string name)
		{
			return new BinExp(name, Type.LIST);
		}

		/// <summary>
		/// Create map bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin a["key"] == "value"
		/// Exp.EQ(
		///     MapExp.GetByKey(MapReturnType.VALUE, Type.STRING, Exp.Val("key"), Exp.MapBin("a")),
		///     Exp.Val("value"));
		/// </code>
		/// </example>
		public static Exp MapBin(string name)
		{
			return new BinExp(name, Type.MAP);
		}

		/// <summary>
		/// Create hll bin expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // HLL bin "a" count > 7
		/// Exp.GT(HLLExp.GetCount(Exp.HLLBin("a")), Exp.Val(7))
		/// </code>
		/// </example>
		public static Exp HLLBin(string name)
		{
			return new BinExp(name, Type.HLL);
		}

		/// <summary>
		/// Create expression that returns if bin of specified name exists.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" exists in record
		/// Exp.BinExists("a")
		/// </code>
		/// </example>
		public static Exp BinExists(string name)
		{
			return Exp.NE(Exp.BinType(name), Exp.Val(0));
		}

		/// <summary>
		/// Create expression that returns bin's integer particle type.
		/// See <see cref="Aerospike.Client.ParticleType"/>.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" particle type is a list
		/// Exp.EQ(Exp.BinType("a"), Exp.Val(ParticleType.LIST))
		/// </code>
		/// </example>
		public static Exp BinType(string name)
		{
			return new CmdStr(BIN_TYPE, name);
		}

		//--------------------------------------------------
		// Misc
		//--------------------------------------------------

		/// <summary>
		/// Create expression that returns record set name string. This expression usually
		/// evaluates quickly because record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record set name == "myset"
		/// Exp.EQ(Exp.SetName(), Exp.Val("myset"))
		/// </code>
		/// </example>
		public static Exp SetName()
		{
			return new Cmd(SET_NAME);
		}

		/// <summary>
		/// Create expression that returns record size on disk. If server storage-engine is
		/// memory, then zero is returned. This expression usually evaluates quickly because
		/// record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record device size >= 100 KB
		/// Exp.GE(Exp.DeviceSize(), Exp.Val(100 * 1024))
		/// </code>
		/// </example>
		public static Exp DeviceSize()
		{
			return new Cmd(DEVICE_SIZE);
		}

		/// <summary>
		/// Create expression that returns record size in memory. If server storage-engine is
		/// not memory nor data-in-memory, then zero is returned. This expression usually evaluates
		/// quickly because record meta data is cached in memory.
		/// <para>
		/// Requires server version 5.3.0+
		/// </para>
		/// </summary>
		/// <example>
		/// <code>
		/// // Record memory size >= 100 KB
		/// Exp.GE(Exp.MemorySize(), Exp.Val(100 * 1024))
		/// </code>
		/// </example>
		public static Exp MemorySize()
		{
			return new Cmd(MEMORY_SIZE);
		}

		/// <summary>
		/// Create expression that returns record last update time expressed as 64 bit integer
		/// nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
		/// record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record last update time >= 2020-01-15
		/// Exp.GE(Exp.LastUpdate(), Exp.Val(new DateTime(2020, 1, 15)))
		/// </code>
		/// </example>
		public static Exp LastUpdate()
		{
			return new Cmd(LAST_UPDATE);
		}

		/// <summary>
		/// Create expression that returns milliseconds since the record was last updated.
		/// This expression usually evaluates quickly because record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record last updated more than 2 hours ago
		/// Exp.GT(Exp.SinceUpdate(), Exp.Val(2 * 60 * 60 * 1000))
		/// </code>
		/// </example>
		public static Exp SinceUpdate()
		{
			return new Cmd(SINCE_UPDATE);
		}

		/// <summary>
		/// Create expression that returns record expiration time expressed as 64 bit integer
		/// nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
		/// record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record expires on 2021-01-01
		/// Exp.And(
		///   Exp.GE(Exp.VoidTime(), Exp.Val(new DateTime(2021, 1, 1))),
		///   Exp.LT(Exp.VoidTime(), Exp.Val(new DateTime(2021, 1, 2))))
		/// </code>
		/// </example>
		public static Exp VoidTime()
		{
			return new Cmd(VOID_TIME);
		}

		/// <summary>
		/// Create expression that returns record expiration time (time to live) in integer seconds.
		/// This expression usually evaluates quickly because record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Record expires in less than 1 hour
		/// Exp.LT(Exp.TTL(), Exp.Val(60 * 60))
		/// </code>
		/// </example>
		public static Exp TTL()
		{
			return new Cmd(CMD_TTL);
		}

		/// <summary>
		/// Create expression that returns if record has been deleted and is still in tombstone state.
		/// This expression usually evaluates quickly because record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Deleted records that are in tombstone state.
		/// Exp.isTombstone()
		/// </code>
		/// </example>
		public static Exp IsTombstone()
		{
			return new Cmd(IS_TOMBSTONE);
		}

		/// <summary>
		/// Create expression that returns record digest modulo as integer. This expression usually
		/// evaluates quickly because record meta data is cached in memory.
		/// </summary>
		/// <example>
		/// <code>
		/// // Records that have digest(key) % 3 == 1
		/// Exp.EQ(Exp.DigestModulo(3), Exp.Val(1))
		/// </code>
		/// </example>
		public static Exp DigestModulo(int mod)
		{
			return new CmdInt(DIGEST_MODULO, mod);
		}

		/// <summary>
		/// Create expression that performs a regex match on a string bin or string value expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // Select string bin "a" that starts with "prefix" and ends with "suffix".
		/// // Ignore case and do not match newline.
		/// Exp.RegexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.StringBin("a"))
		/// </code>
		/// </example>
		/// <param name="regex">regular expression string</param>
		/// <param name="flags">regular expression bit flags. See <see cref="Aerospike.Client.RegexFlag"/></param>
		/// <param name="bin">string bin or string value expression</param>
		public static Exp RegexCompare(string regex, uint flags, Exp bin)
		{
			return new Regex(bin, regex, flags);
		}

		//--------------------------------------------------
		// GEO Spatial
		//--------------------------------------------------

		/// <summary>
		/// Create compare geospatial operation.
		/// </summary>
		/// <example>
		/// <code>
		/// // Query region within coordinates.
		/// string region =
		/// "{ " +
		/// "  \"type\": \"Polygon\", " +
		/// "  \"coordinates\": [ " +
		/// "    [[-122.500000, 37.000000],[-121.000000, 37.000000], " +
		/// "     [-121.000000, 38.080000],[-122.500000, 38.080000], " +
		/// "     [-122.500000, 37.000000]] " +
		/// "    ] " +
		/// "}";
		/// Exp.GeoCompare(Exp.GeoBin("a"), Exp.Geo(region))
		/// </code>
		/// </example>
		public static Exp GeoCompare(Exp left, Exp right)
		{
			return new CmdExp(GEO, left, right);
		}

		/// <summary>
		/// Create geospatial json string value.
		/// </summary>
		public static Exp Geo(string val)
		{
			return new GeoVal(val);
		}

		//--------------------------------------------------
		// Value
		//--------------------------------------------------

		/// <summary>
		/// Create boolean value.
		/// </summary>
		public static Exp Val(bool val)
		{
			return new Bool(val);
		}

		/// <summary>
		/// Create 64 bit integer value.
		/// </summary>
		public static Exp Val(long val)
		{
			return new Int(val);
		}

		/// <summary>
		/// Create 64 bit unsigned integer value.
		/// </summary>
		public static Exp Val(ulong val)
		{
			return new UInt(val);
		}

		/// <summary>
		/// Create Calendar value expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
		/// </summary>
		public static Exp Val(DateTime val)
		{
			return new Int(Util.NanosFromEpoch(val));
		}

		/// <summary>
		/// Create 64 bit floating point value.
		/// </summary>
		public static Exp Val(double val)
		{
			return new Float(val);
		}

		/// <summary>
		/// Create string value.
		/// </summary>
		public static Exp Val(string val)
		{
			return new Str(val);
		}

		/// <summary>
		/// Create blob byte[] value.
		/// </summary>
		public static Exp Val(byte[] val)
		{
			return new Blob(val);
		}

		/// <summary>
		/// Create list value.
		/// </summary>
		public static Exp Val(IList list)
		{
			return new ListVal(list);
		}

		/// <summary>
		/// Create map value.
		/// </summary>
		public static Exp Val(IDictionary map, MapOrder order)
		{
			return new MapVal(map, order);
		}

		/// <summary>
		/// Create nil value.
		/// </summary>
		public static Exp Nil()
		{
			return new NilVal();
		}

		//--------------------------------------------------
		// Boolean Operator
		//--------------------------------------------------

		/// <summary>
		/// Create "not" operator expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // ! (a == 0 || a == 10)
		/// Exp.Not(
		///   Exp.Or(
		///     Exp.EQ(Exp.IntBin("a"), Exp.Val(0)),
		///     Exp.EQ(Exp.IntBin("a"), Exp.Val(10))))
		/// </code>
		/// </example>
		public static Exp Not(Exp exp)
		{
			return new CmdExp(NOT, exp);
		}

		/// <summary>
		/// Create "and" operator that applies to a variable number of expressions.
		/// </summary>
		/// <example>
		/// <code>
		/// // (a > 5 || a == 0) &amp;&amp; b &lt; 3
		/// Exp.And(
		///   Exp.Or(
		///     Exp.GT(Exp.IntBin("a"), Exp.Val(5)),
		///     Exp.EQ(Exp.IntBin("a"), Exp.Val(0))),
		///   Exp.LT(Exp.IntBin("b"), Exp.Val(3)))
		/// </code>
		/// </example>
		public static Exp And(params Exp[] exps)
		{
			return new CmdExp(AND, exps);
		}

		/// <summary>
		/// Create "or" operator that applies to a variable number of expressions.
		/// </summary>
		/// <example>
		/// <code>
		/// // a == 0 || b == 0
		/// Exp.Or(
		///   Exp.EQ(Exp.IntBin("a"), Exp.Val(0)),
		///   Exp.EQ(Exp.IntBin("b"), Exp.Val(0)));
		/// </code>
		/// </example>
		public static Exp Or(params Exp[] exps)
		{
			return new CmdExp(OR, exps);
		}

		/// <summary>
		/// Create expression that returns true if only one of the expressions are true.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // exclusive(a == 0, b == 0)
		/// Exp.Exclusive(
		///   Exp.EQ(Exp.IntBin("a"), Exp.Val(0)),
		///   Exp.EQ(Exp.IntBin("b"), Exp.Val(0)));
		/// </code>
		/// </example>
		public static Exp Exclusive(params Exp[] exps)
		{
			return new CmdExp(EXCLUSIVE, exps);
		}
		
		/// <summary>
		/// Create "equals" expression.
		/// </summary>
		/// <example>
		/// <code>
		/// // a == 11
		/// Exp.EQ(Exp.IntBin("a"), Exp.Val(11))
		/// </code>
		/// </example>
		public static Exp EQ(Exp left, Exp right)
		{
			return new CmdExp(CMD_EQ, left, right);
		}

		/// <summary>
		/// Create "not equal" expression
		/// </summary>
		/// <example>
		/// <code>
		/// // a != 13
		/// Exp.NE(Exp.IntBin("a"), Exp.Val(13))
		/// </code>
		/// </example>
		public static Exp NE(Exp left, Exp right)
		{
			return new CmdExp(CMD_NE, left, right);
		}

		/// <summary>
		/// Create "greater than" operation.
		/// </summary>
		/// <example>
		/// <code>
		/// // a > 8
		/// Exp.GT(Exp.IntBin("a"), Exp.Val(8))
		/// </code>
		/// </example>
		public static Exp GT(Exp left, Exp right)
		{
			return new CmdExp(CMD_GT, left, right);
		}

		/// <summary>
		/// Create "greater than or equal" operation.
		/// </summary>
		/// <example>
		/// <code>
		/// // a >= 88
		/// Exp.GE(Exp.IntBin("a"), Exp.Val(88))
		/// </code>
		/// </example>
		public static Exp GE(Exp left, Exp right)
		{
			return new CmdExp(CMD_GE, left, right);
		}

		/// <summary>
		/// Create "less than" operation.
		/// </summary>
		/// <example>
		/// <code>
		/// // a &lt; 1000
		/// Exp.LT(Exp.IntBin("a"), Exp.Val(1000))
		/// </code>
		/// </example>
		public static Exp LT(Exp left, Exp right)
		{
			return new CmdExp(CMD_LT, left, right);
		}

		/// <summary>
		/// Create "less than or equal" operation.
		/// </summary>
		/// <example>
		/// <code>
		/// // a &lt;= 1
		/// Exp.LE(Exp.IntBin("a"), Exp.Val(1))
		/// </code>
		/// </example>
		public static Exp LE(Exp left, Exp right)
		{
			return new CmdExp(CMD_LE, left, right);
		}

		//--------------------------------------------------
		// Number Operator
		//--------------------------------------------------

		/// <summary>
		/// Create "add" (+) operator that applies to a variable number of expressions.
		/// Return sum of all arguments. All arguments must resolve to the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a + b + c == 10
		/// Exp.EQ(
		///   Exp.Add(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(10));
		/// </code>
		/// </example>
		public static Exp Add(params Exp[] exps)
		{
			return new CmdExp(ADD, exps);
		}

		/// <summary>
		/// Create "subtract" (-) operator that applies to a variable number of expressions.
		/// If only one argument is provided, return the negation of that argument.
		/// Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
		/// argument. All arguments must resolve to the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a - b - c > 10
		/// Exp.GT(
		///   Exp.Sub(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(10));
		/// </code>
		/// </example>
		public static Exp Sub(params Exp[] exps)
		{
			return new CmdExp(SUB, exps);
		}

		/// <summary>
		/// Create "multiply" (*) operator that applies to a variable number of expressions.
		/// Return the product of all arguments. If only one argument is supplied, return
		/// that argument. All arguments must resolve to the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a * b * c &lt; 100
		/// Exp.LT(
		///   Exp.Mul(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(100));
		/// </code>
		/// </example>
		public static Exp Mul(params Exp[] exps)
		{
			return new CmdExp(MUL, exps);
		}

		/// <summary>
		/// Create "divide" (/) operator that applies to a variable number of expressions.
		/// If there is only one argument, returns the reciprocal for that argument.
		/// Otherwise, return the first argument divided by the product of the rest.
		/// All arguments must resolve to the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a / b / c > 1
		/// Exp.GT(
		///   Exp.Div(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(1));
		/// </code>
		/// </example>
		public static Exp Div(params Exp[] exps)
		{
			return new CmdExp(DIV, exps);
		}

		/// <summary>
		/// Create "power" operator that raises a "base" to the "exponent" power.
		/// All arguments must resolve to floats.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // pow(a, 2.0) == 4.0
		/// Exp.EQ(
		///   Exp.Pow(Exp.FloatBin("a"), Exp.Val(2.0)),
		///   Exp.Val(4.0));
		/// </code>
		/// </example>
		public static Exp Pow(Exp @base, Exp exponent)
		{
			return new CmdExp(POW, @base, exponent);
		}

		/// <summary>
		/// Create "log" operator for logarithm of "num" with base "base".
		/// All arguments must resolve to floats.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // log(a, 2.0) == 4.0
		/// Exp.EQ(
		///   Exp.Log(Exp.FloatBin("a"), Exp.Val(2.0)),
		///   Exp.Val(4.0));
		/// </code>
		/// </example>
		public static Exp Log(Exp num, Exp @base)
		{
			return new CmdExp(LOG, num, @base);
		}

		/// <summary>
		/// Create "modulo" (%) operator that determines the remainder of "numerator"
		/// divided by "denominator". All arguments must resolve to integers.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a % 10 == 0
		/// Exp.EQ(
		///   Exp.Mod(Exp.IntBin("a"), Exp.Val(10)),
		///   Exp.Val(0));
		/// </code>
		/// </example>
		public static Exp Mod(Exp numerator, Exp denominator)
		{
			return new CmdExp(MOD, numerator, denominator);
		}

		/// <summary>
		/// Create operator that returns absolute value of a number.
		/// All arguments must resolve to integer or float.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // abs(a) == 1
		/// Exp.EQ(
		///   Exp.Abs(Exp.IntBin("a")),
		///   Exp.Val(1));
		/// </code>
		/// </example>
		public static Exp Abs(Exp value)
		{
			return new CmdExp(ABS, value);
		}

		/// <summary>
		/// Create expression that rounds a floating point number down to the closest integer value.
		/// The return type is float. Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // floor(2.95) == 2.0
		/// Exp.EQ(
		///   Exp.Floor(Exp.Val(2.95)),
		///   Exp.Val(2.0));
		/// </code>
		/// </example>
		public static Exp Floor(Exp num)
		{
			return new CmdExp(FLOOR, num);
		}

		/// <summary>
		/// Create expression that rounds a floating point number up to the closest integer value.
		/// The return type is float. Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // ceil(2.15) >= 3.0
		/// Exp.GE(
		///   Exp.Ceil(Exp.Val(2.15)),
		///   Exp.Val(3.0));
		/// </code>
		/// </example>
		public static Exp Ceil(Exp num)
		{
			return new CmdExp(CEIL, num);
		}

		/// <summary>
		/// Create expression that converts a float to an integer.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // int(2.5) == 2
		/// Exp.EQ(
		///   Exp.ToInt(Exp.Val(2.5)),
		///   Exp.Val(2));
		/// </code>
		/// </example>
		public static Exp ToInt(Exp num)
		{
			return new CmdExp(TO_INT, num);
		}

		/// <summary>
		/// Create expression that converts an integer to a float.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // float(2) == 2.0
		/// Exp.EQ(
		///   Exp.ToFloat(Exp.Val(2))),
		///   Exp.Val(2.0));
		/// </code>
		/// </example>
		public static Exp ToFloat(Exp num)
		{
			return new CmdExp(TO_FLOAT, num);
		}

		/// <summary>
		/// Create integer "and" (&amp;) operator that is applied to two or more integers.
		/// All arguments must resolve to integers.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a &amp; 0xff == 0x11
		/// Exp.EQ(
		///   Exp.IntAnd(Exp.IntBin("a"), Exp.Val(0xff)),
		///   Exp.Val(0x11));
		/// </code>
		/// </example>
		public static Exp IntAnd(params Exp[] exps)
		{
			return new CmdExp(INT_AND, exps);
		}

		/// <summary>
		/// Create integer "or" (|) operator that is applied to two or more integers.
		/// All arguments must resolve to integers.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a | 0x10 != 0
		/// Exp.NE(
		///   Exp.IntOr(Exp.IntBin("a"), Exp.Val(0x10)),
		///   Exp.Val(0));
		/// </code>
		/// </example>
		public static Exp IntOr(params Exp[] exps)
		{
			return new CmdExp(INT_OR, exps);
		}

		/// <summary>
		/// Create integer "xor" (^) operator that is applied to two or more integers.
		/// All arguments must resolve to integers.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a ^ b == 16
		/// Exp.EQ(
		///   Exp.IntXor(Exp.IntBin("a"), Exp.IntBin("b")),
		///   Exp.Val(16));
		/// </code>
		/// </example>
		public static Exp IntXor(params Exp[] exps)
		{
			return new CmdExp(INT_XOR, exps);
		}

		/// <summary>
		/// Create integer "not" (~) operator.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // ~a == 7
		/// Exp.EQ(
		///   Exp.IntNot(Exp.IntBin("a")),
		///   Exp.Val(7));
		/// </code>
		/// </example>
		public static Exp IntNot(Exp exp)
		{
			return new CmdExp(INT_NOT, exp);
		}

		/// <summary>
		/// Create integer "left shift" (&lt;&lt;) operator.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a &lt;&lt; 8 > 0xff
		/// Exp.GT(
		///   Exp.Lshift(Exp.IntBin("a"), Exp.Val(8)),
		///   Exp.Val(0xff));
		/// </code>
		/// </example>
		public static Exp Lshift(Exp value, Exp shift)
		{
			return new CmdExp(INT_LSHIFT, value, shift);
		}

		/// <summary>
		/// Create integer "logical right shift" (>>>) operator.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a >>> 8 > 0xff
		/// Exp.GT(
		///   Exp.Rshift(Exp.IntBin("a"), Exp.Val(8)),
		///   Exp.Val(0xff));
		/// </code>
		/// </example>
		public static Exp Rshift(Exp value, Exp shift)
		{
			return new CmdExp(INT_RSHIFT, value, shift);
		}

		/// <summary>
		/// Create integer "arithmetic right shift" (>>) operator.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // a >> 8 > 0xff
		/// Exp.GT(
		///   Exp.ARshift(Exp.IntBin("a"), Exp.Val(8)),
		///   Exp.Val(0xff));
		/// </code>
		/// </example>
		public static Exp ARshift(Exp value, Exp shift)
		{
			return new CmdExp(INT_ARSHIFT, value, shift);
		}

		/// <summary>
		/// Create expression that returns count of integer bits that are set to 1.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // count(a) == 4
		/// Exp.EQ(
		///   Exp.Count(Exp.IntBin("a")),
		///   Exp.Val(4));
		/// </code>
		/// </example>
		public static Exp Count(Exp exp)
		{
			return new CmdExp(INT_COUNT, exp);
		}

		/// <summary>
		/// Create expression that scans integer bits from left (most significant bit) to
		/// right (least significant bit), looking for a search bit value. When the
		/// search value is found, the index of that bit (where the most significant bit is
		/// index 0) is returned. If "search" is true, the scan will search for the bit
		/// value 1. If "search" is false it will search for bit value 0.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // lscan(a, true) == 4
		/// Exp.EQ(
		///   Exp.Lscan(Exp.IntBin("a"), Exp.Val(true)),
		///   Exp.Val(4));
		/// </code>
		/// </example>
		public static Exp Lscan(Exp value, Exp search)
		{
			return new CmdExp(INT_LSCAN, value, search);
		}

		/// <summary>
		/// Create expression that scans integer bits from right (least significant bit) to
		/// left (most significant bit), looking for a search bit value. When the
		/// search value is found, the index of that bit (where the most significant bit is
		/// index 0) is returned. If "search" is true, the scan will search for the bit
		/// value 1. If "search" is false it will search for bit value 0.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // rscan(a, true) == 4
		/// Exp.EQ(
		///   Exp.Rscan(Exp.IntBin("a"), Exp.Val(true)),
		///   Exp.Val(4));
		/// </code>
		/// </example>
		public static Exp Rscan(Exp value, Exp search)
		{
			return new CmdExp(INT_RSCAN, value, search);
		}

		/// <summary>
		/// Create expression that returns the minimum value in a variable number of expressions.
		/// All arguments must be the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // min(a, b, c) > 0
		/// Exp.GT(
		///   Exp.Min(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(0));
		/// </code>
		/// </example>
		public static Exp Min(params Exp[] exps)
		{
			return new CmdExp(MIN, exps);
		}

		/// <summary>
		/// Create expression that returns the maximum value in a variable number of expressions.
		/// All arguments must be the same type (integer or float).
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // max(a, b, c) > 100
		/// Exp.GT(
		///   Exp.Max(Exp.IntBin("a"), Exp.IntBin("b"), Exp.IntBin("c")),
		///   Exp.Val(100));
		/// </code>
		/// </example>
		public static Exp Max(params Exp[] exps)
		{
			return new CmdExp(MAX, exps);
		}

		//--------------------------------------------------
		// Variables
		//--------------------------------------------------

		/// <summary>
		/// Conditionally select an expression from a variable number of expression pairs
		/// followed by default expression action. Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
		/// // Apply operator based on type.
		/// Exp.cond(
		///   Exp.EQ(Exp.IntBin("type"), Exp.Val(0)), Exp.Add(Exp.IntBin("val1"), Exp.IntBin("val2")),
		///   Exp.EQ(Exp.IntBin("type"), Exp.Val(1)), Exp.Sub(Exp.IntBin("val1"), Exp.IntBin("val2")),
		///   Exp.EQ(Exp.IntBin("type"), Exp.Val(2)), Exp.Mul(Exp.IntBin("val1"), Exp.IntBin("val2")),
		///   Exp.Val(-1));
		/// </code>
		/// </example>
		public static Exp Cond(params Exp[] exps)
		{
			return new CmdExp(COND, exps);
		}

		/// <summary>
		/// Define variables and expressions in scope.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // Args Format: def1, def2, ..., exp
		/// // def: <see cref="Aerospike.Client.Exp.Def(string, Exp)"/>
		/// // exp: Scoped expression
		/// // 5 &lt; a &lt; 10
		/// Exp.Let(
		///   Exp.Def("x", Exp.IntBin("a")),
		///   Exp.And(
		///     Exp.LT(Exp.Val(5), Exp.Var("x")),
		///     Exp.LT(Exp.Var("x"), Exp.Val(10))));
		/// </code>
		/// </example>
		public static Exp Let(params Exp[] exps)
		{
			return new LetExp(exps);
		}

		/// <summary>
		/// Assign variable to a <see cref="Aerospike.Client.Exp.Let(Exp[])"/> 
		/// expression that can be accessed later.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // 5 &lt; a &lt; 10
		/// Exp.Let(
		///   Exp.Def("x", Exp.IntBin("a")),
		///   Exp.And(
		///     Exp.LT(Exp.Val(5), Exp.Var("x")),
		///     Exp.LT(Exp.Var("x"), Exp.Val(10))));
		/// </code>
		/// </example>
		public static Exp Def(string name, Exp value)
		{
			return new DefExp(name, value);
		}

		/// <summary>
		/// Retrieve expression value from a variable.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // 5 &lt; a &lt; 10
		/// Exp.Let(
		///   Exp.Def("x", Exp.IntBin("a")),
		///   Exp.And(
		///     Exp.LT(Exp.Val(5), Exp.Var("x")),
		///     Exp.LT(Exp.Var("x"), Exp.Val(10))));
		/// </code>
		/// </example>
		public static Exp Var(string name)
		{
			return new CmdStr(VAR, name);
		}

		//--------------------------------------------------
		// Miscellaneous
		//--------------------------------------------------

		/// <summary>
		/// Create unknown value. Used to intentionally fail an expression.
		/// The failure can be ignored with <see cref="Aerospike.Client.ExpWriteFlags.EVAL_NO_FAIL"/>
		/// or <see cref="Aerospike.Client.ExpReadFlags.EVAL_NO_FAIL"/>
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <example>
		/// <code>
		/// // double v = balance - 100.0;
		/// // return (v > 0.0)? v : unknown;
		/// Exp.Let(
		///   Exp.Def("v", Exp.Sub(Exp.FloatBin("balance"), Exp.Val(100.0))),
		///   Exp.Cond(
		///     Exp.GE(Exp.var("v"), Exp.Val(0.0)), Exp.Var("v"),
		///     Exp.Unknown()));
		/// </code>
		/// </example>
		public static Exp Unknown()
		{
			return new Cmd(UNKNOWN);
		}

		/// <summary>
		/// Merge precompiled expression into a new expression tree.
		/// Useful for storing common precompiled expressions and then reusing 
		/// these expressions as part of a greater expression.
		/// </summary>
		/// <example>
		/// <code>
		/// Expression e = Exp.Build(Exp.EQ(Exp.IntBin("a"), Exp.Val(200)));
		/// Expression merged = Exp.Build(Exp.And(Exp.Expr(e), Exp.EQ(Exp.IntBin("b"), Exp.Val(100))));
		/// </code>
		/// </example>
		public static Exp Expr(Expression e)
		{
			return new ExpBytes(e);
		}

		//--------------------------------------------------
		// Internal
		//--------------------------------------------------

		private const int UNKNOWN = 0;
		private const int CMD_EQ = 1;
		private const int CMD_NE = 2;
		private const int CMD_GT = 3;
		private const int CMD_GE = 4;
		private const int CMD_LT = 5;
		private const int CMD_LE = 6;
		private const int REGEX = 7;
		private const int GEO = 8;
		private const int AND = 16;
		private const int OR = 17;
		private const int NOT = 18;
		private const int EXCLUSIVE = 19;
		private const int ADD = 20;
		private const int SUB = 21;
		private const int MUL = 22;
		private const int DIV = 23;
		private const int POW = 24;
		private const int LOG = 25;
		private const int MOD = 26;
		private const int ABS = 27;
		private const int FLOOR = 28;
		private const int CEIL = 29;
		private const int TO_INT = 30;
		private const int TO_FLOAT = 31;
		private const int INT_AND = 32;
		private const int INT_OR = 33;
		private const int INT_XOR = 34;
		private const int INT_NOT = 35;
		private const int INT_LSHIFT = 36;
		private const int INT_RSHIFT = 37;
		private const int INT_ARSHIFT = 38;
		private const int INT_COUNT = 39;
		private const int INT_LSCAN = 40;
		private const int INT_RSCAN = 41;
		private const int MIN = 50;
		private const int MAX = 51;
		private const int DIGEST_MODULO = 64;
		private const int DEVICE_SIZE = 65;
		private const int LAST_UPDATE = 66;
		private const int SINCE_UPDATE = 67;
		private const int VOID_TIME = 68;
		private const int CMD_TTL = 69;
		private const int SET_NAME = 70;
		private const int KEY_EXISTS = 71;
		private const int IS_TOMBSTONE = 72;
		private const int MEMORY_SIZE = 73;
		private const int KEY = 80;
		private const int BIN = 81;
		private const int BIN_TYPE = 82;
		private const int COND = 123;
		private const int VAR = 124;
		private const int LET = 125;
		private const int QUOTED = 126;
		private const int CALL = 127;
		public const int MODIFY = 0x40;

		public abstract void Pack(Packer packer);

		/// <summary>
		/// For internal use only.
		/// </summary>
		public class Module : Exp
		{
			internal readonly Exp bin;
			internal readonly byte[] bytes;
			internal readonly int retType;
			internal readonly int module;

			public Module(Exp bin, byte[] bytes, int retType, int module)
			{
				this.bin = bin;
				this.bytes = bytes;
				this.retType = retType;
				this.module = module;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(5);
				packer.PackNumber(Exp.CALL);
				packer.PackNumber(retType);
				packer.PackNumber(module);
				packer.PackByteArray(bytes, 0, bytes.Length);
				bin.Pack(packer);
			}
		}

		private sealed class BinExp : Exp
		{
			internal readonly string name;
			internal readonly Type type;

			public BinExp(string name, Type type)
			{
				this.name = name;
				this.type = type;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(3);
				packer.PackNumber(BIN);
				packer.PackNumber((int)type);
				packer.PackString(name);
			}
		}

		private sealed class Regex : Exp
		{
			internal readonly Exp bin;
			internal readonly string regex;
			internal readonly uint flags;

			internal Regex(Exp bin, string regex, uint flags)
			{
				this.bin = bin;
				this.regex = regex;
				this.flags = flags;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(4);
				packer.PackNumber(REGEX);
				packer.PackNumber(flags);
				packer.PackString(regex);
				bin.Pack(packer);
			}
		}

		private sealed class CmdExp : Exp
		{
			internal readonly Exp[] exps;
			internal readonly int cmd;

			internal CmdExp(int cmd, params Exp[] exps)
			{
				this.exps = exps;
				this.cmd = cmd;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(exps.Length + 1);
				packer.PackNumber(cmd);

				foreach (Exp exp in exps)
				{
					exp.Pack(packer);
				}
			}
		}

		private sealed class LetExp : Exp
		{
			private readonly Exp[] exps;

			internal LetExp(params Exp[] exps)
			{
				this.exps = exps;
			}

			public override void Pack(Packer packer)
			{
				// Let wire format: LET <defname1>, <defexp1>, <defname2>, <defexp2>, ..., <scope exp>
				int count = (exps.Length - 1) * 2 + 2;
				packer.PackArrayBegin(count);
				packer.PackNumber(LET);

				foreach (Exp exp in exps)
				{
					exp.Pack(packer);
				}
			}
		}

		private sealed class DefExp : Exp
		{
			private readonly string name;
			private readonly Exp exp;

			internal DefExp(string name, Exp exp)
			{
				this.name = name;
				this.exp = exp;
			}

			public override void Pack(Packer packer)
			{
				packer.PackString(name);
				exp.Pack(packer);
			}
		}

		private sealed class CmdInt : Exp
		{
			internal readonly int cmd;
			internal readonly int val;

			internal CmdInt(int cmd, int val)
			{
				this.cmd = cmd;
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(2);
				packer.PackNumber(cmd);
				packer.PackNumber(val);
			}
		}

		private sealed class CmdStr : Exp
		{
			internal readonly string str;
			internal readonly int cmd;

			internal CmdStr(int cmd, string str)
			{
				this.str = str;
				this.cmd = cmd;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(2);
				packer.PackNumber(cmd);
				packer.PackString(str);
			}
		}

		private sealed class Cmd : Exp
		{
			internal readonly int cmd;

			internal Cmd(int cmd)
			{
				this.cmd = cmd;
			}

			public override void Pack(Packer packer)
			{
				packer.PackArrayBegin(1);
				packer.PackNumber(cmd);
			}
		}

		private sealed class Bool : Exp
		{
			internal readonly bool val;

			internal Bool(bool val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackBoolean(val);
			}
		}

		private sealed class Int : Exp
		{
			internal readonly long val;

			internal Int(long val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(val);
			}
		}

		private sealed class UInt : Exp
		{
			internal readonly ulong val;

			internal UInt(ulong val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(val);
			}
		}

		private sealed class Float : Exp
		{
			internal readonly double val;

			internal Float(double val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackDouble(val);

			}
		}

		private sealed class Str : Exp
		{
			internal readonly string val;

			internal Str(string val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackParticleString(val);
			}
		}

		private sealed class GeoVal : Exp
		{
			internal readonly string val;

			internal GeoVal(string val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackGeoJSON(val);
			}
		}

		private sealed class Blob : Exp
		{
			internal readonly byte[] val;

			internal Blob(byte[] val)
			{
				this.val = val;
			}

			public override void Pack(Packer packer)
			{
				packer.PackParticleBytes(val);

			}
		}

		public sealed class ListVal : Exp
		{
			internal readonly IList list;

			internal ListVal(IList list)
			{
				this.list = list;
			}

			public override void Pack(Packer packer)
			{
				// List values need an extra array and QUOTED in order to distinguish
				// between a multiple argument array call and a local list.
				packer.PackArrayBegin(2);
				packer.PackNumber(QUOTED);
				packer.PackList(list);
			}
		}

		private sealed class MapVal : Exp
		{
			internal readonly IDictionary map;
			internal readonly MapOrder order;

			internal MapVal(IDictionary map)
			{
				this.map = map;
				this.order = MapOrder.UNORDERED;
			}

			internal MapVal(IDictionary map, MapOrder order)
			{
				this.map = map;
				this.order = order;
			}

			public override void Pack(Packer packer)
			{
				packer.PackMap(map, order);
			}
		}

		private sealed class NilVal : Exp
		{
			public override void Pack(Packer packer)
			{
				packer.PackNil();
			}
		}

		private sealed class ExpBytes : Exp
		{
			internal readonly byte[] bytes;

			internal ExpBytes(Expression e)
			{
				this.bytes = e.Bytes;
			}

			public override void Pack(Packer packer)
			{
				packer.PackByteArray(bytes, 0, bytes.Length);
			}
		}
	}
}
