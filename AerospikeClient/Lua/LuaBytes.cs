/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using LuaInterface;

namespace Aerospike.Client
{
	public class LuaBytes : LuaData
	{
		private byte[] bytes;
		private int type;

		public LuaBytes(byte[] bytes)
		{
			this.bytes = bytes;
		}

		public LuaBytes(int size)
		{
			bytes = new byte[size];
		}

		public LuaBytes()
		{
			bytes = new byte[0];
		}

		public static int size(LuaBytes bytes)
		{
			return bytes.bytes.Length;
		}

		public static void set_size(LuaBytes bytes, int size)
		{
			bytes.bytes = new byte[size];
		}

		public static short get_int16(LuaBytes bytes, int offset)
		{
			return (short)ByteUtil.BytesToShort(bytes.bytes, offset-1);
		}

		public static int get_int32(LuaBytes bytes, int offset)
		{
			return (int)ByteUtil.BytesToInt(bytes.bytes, offset-1);
		}

		public static long get_int64(LuaBytes bytes, int offset)
		{
			return (long)ByteUtil.BytesToLong(bytes.bytes, offset - 1);
		}

		public static void set_int16(LuaBytes bytes, int offset, short value)
		{
			ByteUtil.ShortToBytes((ushort)value, bytes.bytes, offset - 1);
		}

		public static void set_int32(LuaBytes bytes, int offset, int value)
		{
			ByteUtil.IntToBytes((uint)value, bytes.bytes, offset - 1);
		}

		public static void set_int64(LuaBytes bytes, int offset, long value)
		{
			ByteUtil.LongToBytes((ulong)value, bytes.bytes, offset - 1);
		}

		public static void set_string(LuaBytes bytes, int offset, string value)
		{
			ByteUtil.StringToUtf8(value, bytes.bytes, offset - 1);
		}

		public static void set_bytes(LuaBytes bytes, int offset, LuaBytes src)
		{
			Array.Copy(src.bytes, 0, bytes.bytes, offset - 1, src.bytes.Length);
		}

		public static int get_type(LuaBytes bytes)
		{
			return bytes.type;
		}

		public static void set_type(LuaBytes bytes, int type)
		{
			bytes.type = type;
		}

		public static LuaListIterator create_iterator(LuaList list)
		{
			return new LuaListIterator(list.list.GetEnumerator());
		}

		public static object next(LuaListIterator iter)
		{
			return iter.next();
		}

		public byte this[int offset]
		{
			get { return bytes[offset-1]; }
			set { bytes[offset-1] = value; }
		}

		public override string ToString()
		{
			return Util.BytesToString(bytes);
		}

		public object LuaToObject()
		{
			return bytes;
		}

		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaBytes);
			lua.RegisterFunction("bytes.create", null, type.GetConstructor(Type.EmptyTypes));
			lua.RegisterFunction("bytes.create_set", null, type.GetConstructor(new Type[] { typeof(int) }));
			lua.RegisterFunction("bytes.size", null, type.GetMethod("size", new Type[] { type }));
			lua.RegisterFunction("bytes.set_size", null, type.GetMethod("set_size", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int16", null, type.GetMethod("get_int16", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int32", null, type.GetMethod("get_int32", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int64", null, type.GetMethod("get_int64", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.set_int16", null, type.GetMethod("set_int16", new Type[] { type, typeof(int), typeof(short) }));
			lua.RegisterFunction("bytes.set_int32", null, type.GetMethod("set_int32", new Type[] { type, typeof(int), typeof(int) }));
			lua.RegisterFunction("bytes.set_int64", null, type.GetMethod("set_int64", new Type[] { type, typeof(int), typeof(long) }));
			lua.RegisterFunction("bytes.set_string", null, type.GetMethod("set_string", new Type[] { type, typeof(int), typeof(string) }));
			lua.RegisterFunction("bytes.set_bytes", null, type.GetMethod("set_bytes", new Type[] { type, typeof(int), type }));
			lua.RegisterFunction("bytes.get_type", null, type.GetMethod("get_type", new Type[] { type }));
			lua.RegisterFunction("bytes.set_type", null, type.GetMethod("set_type", new Type[] { type, typeof(int) }));
		}
	}
}
