/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using LuaInterface;

namespace Aerospike.Client
{
	public class LuaBytes : LuaData
	{
		private byte[] bytes;
		private int size;
		private int type;

		public LuaBytes(byte[] bytes)
		{
			this.bytes = bytes;
			this.size = bytes.Length;
		}

		public LuaBytes(int capacity)
		{
			bytes = new byte[capacity];
		}

		public LuaBytes()
		{
			bytes = new byte[0];
		}

		public void AppendBigInt16(ushort value)
		{
			SetBigInt16(value, size);
		}

		public void AppendLittleInt16(ushort value)
		{
			SetLittleInt16(value, size);
		}

		public void AppendBigInt32(uint value)
		{
			SetBigInt32(value, size);
		}

		public void AppendLittleInt32(uint value)
		{
			SetLittleInt32(value, size);
		}

		public void AppendBigInt64(ulong value)
		{
			SetBigInt64(value, size);
		}

		public void AppendLittleInt64(ulong value)
		{
			SetLittleInt64(value, size);
		}

		public int AppendVarInt(uint value)
		{
			return SetVarInt(value, size);
		}

		public void AppendString(string value)
		{
			SetString(value, size);
		}

		public void AppendBytes(LuaBytes value, int length)
		{
			SetBytes(value, size, length);
		}

		public void AppendByte(byte value)
		{
			SetByte(value, size);
		}

		public void SetBigInt16(ushort value, int offset)
		{
			int capacity = offset + 2;
			EnsureCapacity(capacity);
			ByteUtil.ShortToBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public void SetLittleInt16(ushort value, int offset)
		{
			int capacity = offset + 2;
			EnsureCapacity(capacity);
			ByteUtil.ShortToLittleBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public void SetBigInt32(uint value, int offset)
		{
			int capacity = offset + 4;
			EnsureCapacity(capacity);
			ByteUtil.IntToBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public void SetLittleInt32(uint value, int offset)
		{
			int capacity = offset + 4;
			EnsureCapacity(capacity);
			ByteUtil.IntToLittleBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public void SetBigInt64(ulong value, int offset)
		{
			int capacity = offset + 8;
			EnsureCapacity(capacity);
			ByteUtil.LongToBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public void SetLittleInt64(ulong value, int offset)
		{
			int capacity = offset + 8;
			EnsureCapacity(capacity);
			ByteUtil.LongToLittleBytes(value, bytes, offset);
			ResetSize(capacity);
		}

		public int SetVarInt(uint value, int offset)
		{
			EnsureCapacity(offset + 5);
			int len = ByteUtil.IntToVarBytes(value, bytes, offset);
			ResetSize(offset + len);
			return len;
		}

		public int SetString(string value, int offset)
		{
			int len = ByteUtil.EstimateSizeUtf8(value);
			EnsureCapacity(offset + len);
			len = ByteUtil.StringToUtf8(value, bytes, offset);
			ResetSize(offset + len);
			return len;
		}

		public void SetBytes(LuaBytes value, int offset, int length)
		{
			if (length == 0 || length > value.size)
			{
				length = value.size;
			}
			int capacity = offset + length;
			EnsureCapacity(capacity);
			Array.Copy(value.bytes, 0, bytes, offset, length);
			ResetSize(capacity);
		}

		public void SetByte(byte value, int offset)
		{
			int capacity = offset + 1;
			EnsureCapacity(capacity);
			bytes[offset] = value;
			ResetSize(capacity);
		}

		public byte GetByte(int offset)
		{
			return bytes[offset];
		}

		public int GetBigInt16(int offset)
		{
			return ByteUtil.BytesToShort(bytes, offset);
		}

		public int GetLittleInt16(int offset)
		{
			return ByteUtil.LittleBytesToShort(bytes, offset);
		}

		public int GetBigInt32(int offset)
		{
			return ByteUtil.BytesToInt(bytes, offset);
		}

		public int GetLittleInt32(int offset)
		{
			return ByteUtil.LittleBytesToInt(bytes, offset);
		}

		public long GetBigInt64(int offset)
		{
			return ByteUtil.BytesToLong(bytes, offset);
		}

		public long GetLittleInt64(int offset)
		{
			return ByteUtil.LittleBytesToLong(bytes, offset);
		}

		public int[] GetVarInt(int offset)
		{
			return ByteUtil.VarBytesToInt(bytes, offset);
		}

		public string GetString(int offset, int length)
		{
			return ByteUtil.Utf8ToString(bytes, offset, length);
		}

		public byte[] GetBytes(int offset, int length)
		{
			byte[] target = new byte[length];
			Array.Copy(bytes, offset, target, 0, length);
			return target;
		}

		private void EnsureCapacity(int capacity)
		{
			if (capacity > bytes.Length)
			{
				int len = bytes.Length * 2;

				if (capacity > len)
				{
					len = capacity;
				}

				byte[] target = new byte[len];
				Array.Copy(bytes, 0, target, 0, size);
				bytes = target;
			}
		}

		private void ResetSize(int capacity)
		{
			if (capacity > size)
			{
				size = capacity;
			}
		}

		public void SetCapacity(int capacity)
		{
			if (bytes.Length == capacity)
			{
				return;
			}

			byte[] target = new byte[capacity];

			if (size > capacity)
			{
				size = capacity;
			}
			Array.Copy(bytes, 0, target, 0, size);
			bytes = target;
		}

		public object LuaToObject()
		{
			return bytes;
		}

		public int GetType()
		{
			return type;
		}

		public void SetType(int type)
		{
			this.type = type;
		}

		public int Size()
		{
			return size;
		}

		public override string ToString()
		{
			return ByteUtil.BytesToHexString(bytes, 0, size);
		}

		public static int get_size(LuaBytes bytes)
		{
			return bytes.Size();
		}

		public static void set_size(LuaBytes bytes, int capacity)
		{
			bytes.SetCapacity(capacity);
		}

		public static int get_type(LuaBytes bytes)
		{
			return bytes.GetType();
		}

		public static void set_type(LuaBytes bytes, int type)
		{
			bytes.SetType(type);
		}
		
		public static string get_string(LuaBytes bytes, int offset, int length)
		{
			return bytes.GetString(offset - 1, length);
		}

		public static LuaBytes get_bytes(LuaBytes bytes, int offset, int length)
		{
			byte[] b = bytes.GetBytes(offset - 1, length);
			return new LuaBytes(b);
		}

		public static byte get_byte(LuaBytes bytes, int offset)
		{
			return bytes.GetByte(offset - 1);
		}

		public static int get_int16_be(LuaBytes bytes, int offset)
		{
			return bytes.GetBigInt16(offset - 1);
		}

		public static int get_int16_le(LuaBytes bytes, int offset)
		{
			return bytes.GetLittleInt16(offset - 1);
		}
		
		public static int get_int32_be(LuaBytes bytes, int offset)
		{
			return bytes.GetBigInt32(offset - 1);
		}

		public static int get_int32_le(LuaBytes bytes, int offset)
		{
			return bytes.GetLittleInt32(offset - 1);
		}

		public static long get_int64_be(LuaBytes bytes, int offset)
		{
			return bytes.GetBigInt64(offset - 1);
		}

		public static long get_int64_le(LuaBytes bytes, int offset)
		{
			return bytes.GetLittleInt64(offset - 1);
		}

		public static int get_var_int(LuaBytes bytes, int offset)
		{
			// TODO: return both results.
			int[] results = bytes.GetVarInt(offset - 1);
			return results[0];
		}

		public static int set_string(LuaBytes bytes, int offset, string value)
		{
			return bytes.SetString(value, offset - 1);
		}

		public static void set_bytes(LuaBytes bytes, int offset, LuaBytes src, int length)
		{
			bytes.SetBytes(src, offset - 1, length);
		}

		public static void set_byte(LuaBytes bytes, int offset, byte value)
		{
			bytes.SetByte(value, offset - 1);
		}

		public static void set_int16_be(LuaBytes bytes, int offset, ushort value)
		{
			bytes.SetBigInt16(value, offset - 1);
		}

		public static void set_int16_le(LuaBytes bytes, int offset, ushort value)
		{
			bytes.SetLittleInt16(value, offset - 1);
		}

		public static void set_int32_be(LuaBytes bytes, int offset, uint value)
		{
			bytes.SetBigInt32(value, offset - 1);
		}

		public static void set_int32_le(LuaBytes bytes, int offset, uint value)
		{
			bytes.SetLittleInt32(value, offset - 1);
		}

		public static void set_int64_be(LuaBytes bytes, int offset, ulong value)
		{
			bytes.SetBigInt64(value, offset - 1);
		}

		public static void set_int64_le(LuaBytes bytes, int offset, ulong value)
		{
			bytes.SetLittleInt64(value, offset - 1);
		}

		public static int set_var_int(LuaBytes bytes, int offset, uint value)
		{
			return bytes.SetVarInt(value, offset - 1);
		}

		public static void append_string(LuaBytes bytes, string value)
		{
			bytes.AppendString(value);
		}

		public static void append_bytes(LuaBytes bytes, LuaBytes src, int length)
		{
			bytes.AppendBytes(src, length);
		}

		public static void append_byte(LuaBytes bytes, byte value)
		{
			bytes.AppendByte(value);
		}

		public static void append_int16_be(LuaBytes bytes, ushort value)
		{
			bytes.AppendBigInt16(value);
		}

		public static void append_int16_le(LuaBytes bytes, ushort value)
		{
			bytes.AppendLittleInt16(value);
		}

		public static void append_int32_be(LuaBytes bytes, uint value)
		{
			bytes.AppendBigInt32(value);
		}

		public static void append_int32_le(LuaBytes bytes, uint value)
		{
			bytes.AppendLittleInt32(value);
		}

		public static void append_int64_be(LuaBytes bytes, ulong value)
		{
			bytes.AppendBigInt64(value);
		}

		public static void append_int64_le(LuaBytes bytes, ulong value)
		{
			bytes.AppendLittleInt64(value);
		}

		public static int append_var_int(LuaBytes bytes, uint value)
		{
			return bytes.AppendVarInt(value);
		}

		public byte this[int offset]
		{
			get { return bytes[offset-1]; }
			set { bytes[offset-1] = value; }
		}

		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaBytes);
			lua.RegisterFunction("bytes.create", null, type.GetConstructor(Type.EmptyTypes));
			lua.RegisterFunction("bytes.create_set", null, type.GetConstructor(new Type[] { typeof(int) }));
			lua.RegisterFunction("bytes.size", null, type.GetMethod("get_size", new Type[] { type }));
			lua.RegisterFunction("bytes.set_size", null, type.GetMethod("set_size", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_type", null, type.GetMethod("get_type", new Type[] { type }));
			lua.RegisterFunction("bytes.set_type", null, type.GetMethod("set_type", new Type[] { type, typeof(int) }));

			lua.RegisterFunction("bytes.get_string", null, type.GetMethod("get_string", new Type[] { type, typeof(int), typeof(int) }));
			lua.RegisterFunction("bytes.get_bytes", null, type.GetMethod("get_bytes", new Type[] { type, typeof(int), typeof(int) }));
			lua.RegisterFunction("bytes.get_byte", null, type.GetMethod("get_byte", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int16", null, type.GetMethod("get_int16_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int16_be", null, type.GetMethod("get_int16_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int16_le", null, type.GetMethod("get_int16_le", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int32", null, type.GetMethod("get_int32_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int32_be", null, type.GetMethod("get_int32_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int32_le", null, type.GetMethod("get_int32_le", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int64", null, type.GetMethod("get_int64_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int64_be", null, type.GetMethod("get_int64_be", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_int64_le", null, type.GetMethod("get_int64_le", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("bytes.get_var_int", null, type.GetMethod("get_var_int", new Type[] { type, typeof(int) }));

			lua.RegisterFunction("bytes.set_string", null, type.GetMethod("set_string", new Type[] { type, typeof(int), typeof(string) }));
			lua.RegisterFunction("bytes.set_bytes", null, type.GetMethod("set_bytes", new Type[] { type, typeof(int), type, typeof(int) }));
			lua.RegisterFunction("bytes.set_byte", null, type.GetMethod("set_byte", new Type[] { type, typeof(int), typeof(byte) }));
			lua.RegisterFunction("bytes.set_int16", null, type.GetMethod("set_int16_be", new Type[] { type, typeof(int), typeof(ushort) }));
			lua.RegisterFunction("bytes.set_int16_be", null, type.GetMethod("set_int16_be", new Type[] { type, typeof(int), typeof(ushort) }));
			lua.RegisterFunction("bytes.set_int16_le", null, type.GetMethod("set_int16_le", new Type[] { type, typeof(int), typeof(ushort) }));
			lua.RegisterFunction("bytes.set_int32", null, type.GetMethod("set_int32_be", new Type[] { type, typeof(int), typeof(uint) }));
			lua.RegisterFunction("bytes.set_int32_be", null, type.GetMethod("set_int32_be", new Type[] { type, typeof(int), typeof(uint) }));
			lua.RegisterFunction("bytes.set_int32_le", null, type.GetMethod("set_int32_le", new Type[] { type, typeof(int), typeof(uint) }));
			lua.RegisterFunction("bytes.set_int64", null, type.GetMethod("set_int64_be", new Type[] { type, typeof(int), typeof(ulong) }));
			lua.RegisterFunction("bytes.set_int64_be", null, type.GetMethod("set_int64_be", new Type[] { type, typeof(int), typeof(ulong) }));
			lua.RegisterFunction("bytes.set_int64_le", null, type.GetMethod("set_int64_le", new Type[] { type, typeof(int), typeof(ulong) }));
			lua.RegisterFunction("bytes.set_var_int", null, type.GetMethod("set_var_int", new Type[] { type, typeof(int), typeof(uint) }));

			lua.RegisterFunction("bytes.append_string", null, type.GetMethod("append_string", new Type[] { type, typeof(string) }));
			lua.RegisterFunction("bytes.append_bytes", null, type.GetMethod("append_bytes", new Type[] { type, type, typeof(int) }));
			lua.RegisterFunction("bytes.append_byte", null, type.GetMethod("append_byte", new Type[] { type, typeof(byte) }));
			lua.RegisterFunction("bytes.append_int16", null, type.GetMethod("append_int16_be", new Type[] { type, typeof(ushort) }));
			lua.RegisterFunction("bytes.append_int16_be", null, type.GetMethod("append_int16_be", new Type[] { type, typeof(ushort) }));
			lua.RegisterFunction("bytes.append_int16_le", null, type.GetMethod("append_int16_le", new Type[] { type, typeof(ushort) }));
			lua.RegisterFunction("bytes.append_int32", null, type.GetMethod("append_int32_be", new Type[] { type, typeof(uint) }));
			lua.RegisterFunction("bytes.append_int32_be", null, type.GetMethod("append_int32_be", new Type[] { type, typeof(uint) }));
			lua.RegisterFunction("bytes.append_int32_le", null, type.GetMethod("append_int32_le", new Type[] { type, typeof(uint) }));
			lua.RegisterFunction("bytes.append_int64", null, type.GetMethod("append_int64_be", new Type[] { type, typeof(ulong) }));
			lua.RegisterFunction("bytes.append_int64_be", null, type.GetMethod("append_int64_be", new Type[] { type, typeof(ulong) }));
			lua.RegisterFunction("bytes.append_int64_le", null, type.GetMethod("append_int64_le", new Type[] { type, typeof(ulong) }));
			lua.RegisterFunction("bytes.append_var_int", null, type.GetMethod("append_var_int", new Type[] { type, typeof(uint) }));
		}
	}
}
