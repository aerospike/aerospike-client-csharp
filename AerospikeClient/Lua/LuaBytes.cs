/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using Neo.IronLua;

namespace Aerospike.Client
{
	public class LuaBytes : LuaData
	{
		private byte[] bytes;
		private int length;
		private int type;

		public LuaBytes(byte[] bytes)
		{
			this.bytes = bytes;
			this.length = bytes.Length;
		}

		public LuaBytes(int capacity)
		{
			bytes = new byte[capacity];
		}

		public LuaBytes()
		{
			bytes = new byte[0];
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

		public void SetBytes(LuaBytes value, int offset, int len)
		{
			if (len == 0 || len > value.length)
			{
				len = value.length;
			}
			int capacity = offset + len;
			EnsureCapacity(capacity);
			Array.Copy(value.bytes, 0, bytes, offset, len);
			ResetSize(capacity);
		}

		public void SetByte(byte value, int offset)
		{
			int capacity = offset + 1;
			EnsureCapacity(capacity);
			bytes[offset] = value;
			ResetSize(capacity);
		}

		public string GetString(int offset, int len)
		{
			if (offset < 0 || offset >= this.length)
			{
				return "";
			}

			if (offset + len > this.length)
			{
				len = this.length - offset;
			}
			return ByteUtil.Utf8ToString(bytes, offset, len);
		}

		public byte[] GetBytes(int offset, int len)
		{
			if (offset < 0 || offset >= this.length)
			{
				return new byte[0];
			}

			if (offset + len > this.length)
			{
				len = this.length - offset;
			}
			byte[] target = new byte[len];
			Array.Copy(bytes, offset, target, 0, len);
			return target;
		}

		public byte GetByte(int offset)
		{
			return (offset >= 0 && offset < length) ? bytes[offset] : (byte)0;
		}

		public int GetShortBig(int offset)
		{
			if (offset < 0 || offset > this.length)
			{
				return 0;
			}
			return ByteUtil.BytesToShort(bytes, offset);
		}

		public int GetShortLittle(int offset)
		{
			if (offset < 0 || offset > this.length)
			{
				return 0;
			}
			return ByteUtil.LittleBytesToShort(bytes, offset);
		}

		public int GetIntBig(int offset)
		{
			if (offset < 0 || offset + 4 > this.length)
			{
				return 0;
			}
			return ByteUtil.BytesToInt(bytes, offset);
		}

		public int GetIntLittle(int offset)
		{
			if (offset < 0 || offset + 4 > this.length)
			{
				return 0;
			}
			return ByteUtil.LittleBytesToInt(bytes, offset);
		}

		public long GetLongBig(int offset)
		{
			if (offset < 0 || offset + 8 > this.length)
			{
				return 0;
			}
			return ByteUtil.BytesToLong(bytes, offset);
		}

		public long GetLongLittle(int offset)
		{
			if (offset < 0 || offset + 8 > this.length)
			{
				return 0;
			}
			return ByteUtil.LittleBytesToLong(bytes, offset);
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
				Array.Copy(bytes, 0, target, 0, length);
				bytes = target;
			}
		}

		private void ResetSize(int capacity)
		{
			if (capacity > length)
			{
				length = capacity;
			}
		}

		public void SetCapacity(int capacity)
		{
			if (bytes.Length == capacity)
			{
				return;
			}

			byte[] target = new byte[capacity];

			if (length > capacity)
			{
				length = capacity;
			}
			Array.Copy(bytes, 0, target, 0, length);
			bytes = target;
		}

		public byte this[int offset]
		{
			get { return GetByte(offset - 1); }
			set { SetByte(value, offset - 1); }
		}

		public object LuaToObject()
		{
			return bytes;
		}

		public override string ToString()
		{
			return ByteUtil.BytesToHexString(bytes, 0, length);
		}

		public static int size(LuaBytes bytes)
		{
			return bytes.length;
		}

		public static void set_size(LuaBytes bytes, int capacity)
		{
			bytes.SetCapacity(capacity);
		}

		public static int get_type(LuaBytes bytes)
		{
			return bytes.type;
		}

		public static void set_type(LuaBytes bytes, int type)
		{
			bytes.type = type;
		}
		
		public static string get_string(LuaBytes bytes, int offset, int len)
		{
			return bytes.GetString(offset - 1, len);
		}

		public static LuaBytes get_bytes(LuaBytes bytes, int offset, int len)
		{
			byte[] b = bytes.GetBytes(offset - 1, len);
			return new LuaBytes(b);
		}

		public static byte get_byte(LuaBytes bytes, int offset)
		{
			return bytes.GetByte(offset-1);
		}

		public static int get_int16(LuaBytes bytes, int offset)
		{
			return bytes.GetShortBig(offset - 1);
		}

		public static int get_int16_be(LuaBytes bytes, int offset)
		{
			return bytes.GetShortBig(offset - 1);
		}

		public static int get_int16_le(LuaBytes bytes, int offset)
		{
			return bytes.GetShortLittle(offset - 1);
		}

		public static int get_int32(LuaBytes bytes, int offset)
		{
			return bytes.GetIntBig(offset - 1);
		}

		public static int get_int32_be(LuaBytes bytes, int offset)
		{
			return bytes.GetIntBig(offset - 1);
		}

		public static int get_int32_le(LuaBytes bytes, int offset)
		{
			return bytes.GetIntLittle(offset - 1);
		}

		public static long get_int64(LuaBytes bytes, int offset)
		{
			return bytes.GetLongBig(offset - 1);
		}

		public static long get_int64_be(LuaBytes bytes, int offset)
		{
			return bytes.GetLongBig(offset - 1);
		}

		public static long get_int64_le(LuaBytes bytes, int offset)
		{
			return bytes.GetLongLittle(offset - 1);
		}

		public static int get_var_int(LuaBytes bytes, int offset, out int size)
		{
			offset--;

			if (offset < 0 || offset > bytes.length)
			{
				size = 0;
				return 0;
			}
			return ByteUtil.VarBytesToInt(bytes.bytes, offset, out size);
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

		public static void set_int16(LuaBytes bytes, int offset, ushort value)
		{
			bytes.SetBigInt16(value, offset - 1);
		}

		public static void set_int16_be(LuaBytes bytes, int offset, ushort value)
		{
			bytes.SetBigInt16(value, offset - 1);
		}

		public static void set_int16_le(LuaBytes bytes, int offset, ushort value)
		{
			bytes.SetLittleInt16(value, offset - 1);
		}

		public static void set_int32(LuaBytes bytes, int offset, uint value)
		{
			bytes.SetBigInt32(value, offset - 1);
		}

		public static void set_int32_be(LuaBytes bytes, int offset, uint value)
		{
			bytes.SetBigInt32(value, offset - 1);
		}

		public static void set_int32_le(LuaBytes bytes, int offset, uint value)
		{
			bytes.SetLittleInt32(value, offset - 1);
		}

		public static void set_int64(LuaBytes bytes, int offset, ulong value)
		{
			bytes.SetBigInt64(value, offset - 1);
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
			bytes.SetString(value, bytes.length);
		}

		public static void append_bytes(LuaBytes bytes, LuaBytes src, int length)
		{
			bytes.SetBytes(src, bytes.length, length);
		}

		public static void append_byte(LuaBytes bytes, byte value)
		{
			bytes.SetByte(value, bytes.length);
		}

		public static void append_int16(LuaBytes bytes, ushort value)
		{
			bytes.SetBigInt16(value, bytes.length);
		}

		public static void append_int16_be(LuaBytes bytes, ushort value)
		{
			bytes.SetBigInt16(value, bytes.length);
		}

		public static void append_int16_le(LuaBytes bytes, ushort value)
		{
			bytes.SetLittleInt16(value, bytes.length);
		}

		public static void append_int32(LuaBytes bytes, uint value)
		{
			bytes.SetBigInt32(value, bytes.length);
		}

		public static void append_int32_be(LuaBytes bytes, uint value)
		{
			bytes.SetBigInt32(value, bytes.length);
		}

		public static void append_int32_le(LuaBytes bytes, uint value)
		{
			bytes.SetLittleInt32(value, bytes.length);
		}

		public static void append_int64(LuaBytes bytes, ulong value)
		{
			bytes.SetBigInt64(value, bytes.length);
		}

		public static void append_int64_be(LuaBytes bytes, ulong value)
		{
			bytes.SetBigInt64(value, bytes.length);
		}

		public static void append_int64_le(LuaBytes bytes, ulong value)
		{
			bytes.SetLittleInt64(value, bytes.length);
		}

		public static int append_var_int(LuaBytes bytes, uint value)
		{
			return bytes.SetVarInt(value, bytes.length);
		}
	}
}
