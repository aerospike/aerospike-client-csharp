/*
 * Copyright 2012-2020 Aerospike, Inc.
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

		/// <summary>
		/// Create final expression that contains packed byte instructions used in the wire protocol.
		/// </summary>
		public static Expression Build(Exp exp)
		{
			return new Expression(exp);
		}

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
		/// Exp.GeoCompare(Exp.GeoBin("loc"), Exp.Val(region))
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
		/// This method requires Aerospike Server version >= 5.3.0.
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
		public static Exp Val(IDictionary map)
		{
			return new MapVal(map);
		}

		/// <summary>
		/// Create nil value.
		/// </summary>
		public static Exp Nil()
		{
			return new NilVal();
		}

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
		// Internal
		//--------------------------------------------------

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

			internal MapVal(IDictionary map)
			{
				this.map = map;
			}

			public override void Pack(Packer packer)
			{
				packer.PackMap(map);
			}
		}

		private sealed class NilVal : Exp
		{
			public override void Pack(Packer packer)
			{
				packer.PackNil();
			}
		}
	}
}
