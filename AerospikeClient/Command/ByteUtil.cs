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
using System.IO;
using System.Text;
using System.Numerics;
using System.Runtime.Serialization;

namespace Aerospike.Client
{
	public sealed class ByteUtil
	{
		public static Value BytesToKeyValue(int type, byte[] buf, int offset, int len)
		{
			switch (type)
			{
				case ParticleType.STRING:
					return Value.Get(Utf8ToString(buf, offset, len));

				case ParticleType.INTEGER:
					return BytesToLongValue(buf, offset, len);

				case ParticleType.BLOB:
					byte[] dest = new byte[len];
					Array.Copy(buf, offset, dest, 0, len);
					return Value.Get(dest);

				default:
					return null;
			}
		}
		
		public static object BytesToParticle(int type, byte[] buf, int offset, int len)
		{
			switch (type)
			{
				case ParticleType.STRING:
					return Utf8ToString(buf, offset, len);

				case ParticleType.INTEGER:
					return BytesToNumber(buf, offset, len);

				case ParticleType.BLOB:
					byte[] dest = new byte[len];
					Array.Copy(buf, offset, dest, 0, len);
					return dest;

				case ParticleType.CSHARP_BLOB:
					return BytesToObject(buf, offset, len);

				case ParticleType.LIST:
				{
					Unpacker unpacker = new Unpacker(buf, offset, len, false);
					return unpacker.UnpackList();
				}

				case ParticleType.MAP:
				{
					Unpacker unpacker = new Unpacker(buf, offset, len, false);
					return unpacker.UnpackMap();
				}

				default:
					return null;
			}
		}

		/// <summary>
		/// Estimate size of Utf8 encoded bytes without performing the actual encoding. 
		/// </summary>
		public static int EstimateSizeUtf8(string s)
		{
			if (s == null)
			{
				return 0;
			}
			// The system library encoding is optimized, so there is no need to write a custom implementation.
			return Encoding.UTF8.GetByteCount(s);
		}

		/// <summary>
		/// Convert input string to UTF-8 and return corresponding byte array.
		/// </summary>
		public static byte[] StringToUtf8(string s)
		{
			if (s == null || s.Length == 0)
			{
				return new byte[0];
			}
			return Encoding.UTF8.GetBytes(s);
		}
		
		/// <summary>
		/// Convert input string to UTF-8, copies into buffer (at given offset).
		/// Returns number of bytes in the string.
		/// </summary>
		public static int StringToUtf8(string s, byte[] buf, int offset)
		{
			if (s == null)
			{
				return 0;
			}
			// The system library encoding is optimized, so there is no need to write a custom implementation.
			byte[] data = Encoding.UTF8.GetBytes(s);
			Array.Copy(data, 0, buf, offset, data.Length);
			return data.Length;
		}

		/// <summary>
		/// Convert UTF8 byte array into a string.
		/// </summary>
		public static string Utf8ToString(byte[] buf, int offset, int length)
		{
			// The system library encoding is optimized, so there is no need to write a custom implementation.
			return Encoding.UTF8.GetString(buf, offset, length);
		}

		/// <summary>
		/// Convert UTF8 numeric digits to an unsigned integer.  Negative integers are not supported.
		/// <para>
		/// Input format: 1234
		/// </para>
		/// </summary>
		public static uint Utf8DigitsToInt(byte[] buf, int begin, int end)
		{
			uint val = 0;
			uint mult = 1;

			for (int i = end - 1; i >= begin; i--)
			{
				val += ((uint)buf[i] - 48) * mult;
				mult *= 10;
			}
			return val;
		}

		public static string BytesToHexString(byte[] buf)
		{
			if (buf == null || buf.Length == 0)
			{
				return "";
			}
			StringBuilder sb = new StringBuilder(buf.Length * 2);

			for (int i = 0; i < buf.Length; i++)
			{
				sb.Append(string.Format("{0:x2}", buf[i]));
			}
			return sb.ToString();
		}

		public static string BytesToHexString(byte[] buf, int offset, int len)
		{
			StringBuilder sb = new StringBuilder(len * 2);

			for (int i = offset; i < len; i++)
			{
				sb.Append(string.Format("{0:x2}", buf[i]));
			}
			return sb.ToString();
		}

		public static object BytesToObject(byte[] buf, int offset, int len)
		{
			if (len <= 0)
			{
				return null;
			}

			try
			{
				using (MemoryStream ms = new MemoryStream())
				{
					ms.Write(buf, offset, len);
					ms.Seek(0, 0);
					return Formatter.Default.Deserialize(ms);
				}
			}
			catch (SerializationException se)
			{
				throw new AerospikeException.Serialize(se);
			}
		}

		public static object BytesToNumber(byte[] buf, int offset, int len)
		{
			// Server always returns 8 for integer length.
			if (len == 8)
			{
				return BytesToLong(buf, offset);
			}

			// Handle other lengths just in case server changes.
			if (len < 8)
			{
				// Handle variable length long. 
				long val = 0;

				for (int i = 0; i < len; i++)
				{
					val <<= 8;
					val |= buf[offset + i];
				}
				return val;
			}

			// Handle huge numbers.
			return BytesToBigInteger(buf, offset, len);
		}

		public static Value BytesToLongValue(byte[] buf, int offset, int len)
		{
			long val = 0;

			for (int i = 0; i < len; i++)
			{
				val <<= 8;
				val |= buf[offset + i];
			}
			return new Value.LongValue(val);
		}

		public static object BytesToBigInteger(byte[] buf, int offset, int len)
		{
			bool negative = false;

			if ((buf[offset] & 0x80) != 0)
			{
				negative = true;
				buf[offset] &= 0x7f;
			}
			byte[] bytes = new byte[len];
			Array.Copy(buf, offset, bytes, 0, len);

			BigInteger big = new BigInteger(bytes);

			if (negative)
			{
				big = -big;
			}
			return big;
		}

		//-------------------------------------------------------
		// 64 bit floating point conversions.
		//-------------------------------------------------------

		public static int DoubleToBytes(double v, byte[] buf, int offset)
		{
			return ByteUtil.LongToBytes((ulong)BitConverter.DoubleToInt64Bits(v), buf, offset);
		}

		public static double BytesToDouble(byte[] buf, int offset)
		{
			return BitConverter.Int64BitsToDouble(BytesToLong(buf, offset));
		}

		public static int FloatToBytes(float v, byte[] buf, int offset)
		{
			byte[] bytes = BitConverter.GetBytes(v);

			buf[offset++] = bytes[3];
			buf[offset++] = bytes[2];
			buf[offset++] = bytes[1];
			buf[offset++] = bytes[0];

			return 4;
		}

		public static float BytesToFloat(byte[] buf, int offset)
		{
			byte[] bytes = new byte[4];

			bytes[0] = buf[offset + 3];
			bytes[1] = buf[offset + 2];
			bytes[2] = buf[offset + 1];
			bytes[3] = buf[offset];

			return BitConverter.ToSingle(bytes, 0);
		}

		//-------------------------------------------------------
		// 64 bit number conversions.
		//-------------------------------------------------------

		/// <summary>
		/// Convert ulong to big endian 64 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int LongToBytes(ulong v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset++] = (byte)(v >> 56);
			buf[offset++] = (byte)(v >> 48);
			buf[offset++] = (byte)(v >> 40);
			buf[offset++] = (byte)(v >> 32);
			buf[offset++] = (byte)(v >> 24);
			buf[offset++] = (byte)(v >> 16);
			buf[offset++] = (byte)(v >>  8);
			buf[offset]   = (byte)(v >>  0);
			return 8;
		}

		/// <summary>
		/// Convert long to little endian 64 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int LongToLittleBytes(ulong v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine. 
			buf[offset++] = (byte)(v >> 0);
			buf[offset++] = (byte)(v >> 8);
			buf[offset++] = (byte)(v >> 16);
			buf[offset++] = (byte)(v >> 24);
			buf[offset++] = (byte)(v >> 32);
			buf[offset++] = (byte)(v >> 40);
			buf[offset++] = (byte)(v >> 48);
			buf[offset]   = (byte)(v >> 56);		
			return 8;
		}

		/// <summary>
		/// Convert big endian signed 64 bits to long.
		/// </summary>
		public static long BytesToLong(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is slightly faster than System.BitConverter.ToInt64().
			// Assume little endian machine and reverse/convert in one pass. 
			return (long)(
				((ulong)(buf[offset]) << 56) |
				((ulong)(buf[offset + 1]) << 48) |
				((ulong)(buf[offset + 2]) << 40) |
				((ulong)(buf[offset + 3]) << 32) |
				((ulong)(buf[offset + 4]) << 24) |
				((ulong)(buf[offset + 5]) << 16) |
				((ulong)(buf[offset + 6]) << 8) |
				((ulong)(buf[offset + 7]) << 0)
				);
		}

		/// <summary>
		/// Convert little endian signed 64 bits to long.
		/// </summary>
		public static long LittleBytesToLong(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is slightly faster than System.BitConverter.ToInt64().
			// Assume little endian machine.
			return (long)(
			   ((ulong)(buf[offset]) << 0) |
			   ((ulong)(buf[offset + 1]) << 8) |
			   ((ulong)(buf[offset + 2]) << 16) |
			   ((ulong)(buf[offset + 3]) << 24) |
			   ((ulong)(buf[offset + 4]) << 32) |
			   ((ulong)(buf[offset + 5]) << 40) |
			   ((ulong)(buf[offset + 6]) << 48) |
			   ((ulong)(buf[offset + 7]) << 56)
			   );
		}

		//-------------------------------------------------------
		// 32 bit number conversions.
		//-------------------------------------------------------

		/// <summary>
		/// Convert int to big endian 32 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int IntToBytes(uint v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset++] = (byte)(v >> 24);
			buf[offset++] = (byte)(v >> 16);
			buf[offset++] = (byte)(v >> 8);
			buf[offset]   = (byte)(v >> 0);
			return 4;
		}

		/// <summary>
		/// Convert int to little endian 32 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int IntToLittleBytes(uint v, byte[] buf, int offset)
		{
			buf[offset++] = (byte)(v >> 0);
			buf[offset++] = (byte)(v >> 8);
			buf[offset++] = (byte)(v >> 16);
			buf[offset]   = (byte)(v >> 24);
			return 4;
		}

		/// <summary>
		/// Convert big endian signed 32 bits to int.
		/// </summary>
		public static int BytesToInt(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt32().
			// Assume little endian machine and reverse/convert in one pass. 
			return (((buf[offset]) << 24) | 
				    ((buf[offset + 1]) << 16) | 
					((buf[offset + 2]) << 8) | 
					 (buf[offset + 3]));
		}

		/// <summary>
		/// Convert little endian signed 32 bits to int.
		/// </summary>
		public static int LittleBytesToInt(byte[] buf, int offset)
		{
			return ((buf[offset]) |
					((buf[offset + 1]) << 8) |
					((buf[offset + 2]) << 16) |
					((buf[offset + 3]) << 24));
		}

		/// <summary>
		/// Convert big endian unsigned 32 bits to uint.
		/// </summary>
		public static uint BytesToUInt(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToUInt32().
			// Assume little endian machine and reverse/convert in one pass.
			return (
				((uint)(buf[offset]) << 24) |
				((uint)(buf[offset + 1]) << 16) |
				((uint)(buf[offset + 2]) << 8) |
				((uint)(buf[offset + 3]))
				);
		}

		//-------------------------------------------------------
		// 16 bit number conversions.
		//-------------------------------------------------------

		/// <summary>
		/// Convert int to big endian 16 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int ShortToBytes(ushort v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset++] = (byte)(v >> 8);
			buf[offset] = (byte)(v >> 0);
			return 2;
		}

		/// <summary>
		/// Convert int to little endian 16 bits.
		/// The bit pattern will be the same regardless of sign.
		/// </summary>
		public static int ShortToLittleBytes(ushort v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset++] = (byte)(v >> 0);
			buf[offset]   = (byte)(v >> 8);
			return 2;
		}

		/// <summary>
		/// Convert big endian unsigned 16 bits to int.
		/// </summary>
		public static int BytesToShort(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt16().
			// Assume little endian machine and reverse/convert in one pass. 
			return (
				((buf[offset]) << 8) |
				((buf[offset + 1]) << 0)
				);
		}

		/// <summary>
		/// Convert little endian unsigned 16 bits to int.
		/// </summary>
		public static int LittleBytesToShort(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt16().
			// Assume little endian machine and reverse/convert in one pass. 
			return (
				((buf[offset]) << 0) |
				((buf[offset + 1]) << 8)
				);
		}

		//-------------------------------------------------------
		// Variable byte number conversions.
		//-------------------------------------------------------

		/// <summary>
		/// Encode an integer in variable 7-bit format.
		/// The high bit indicates if more bytes are used.
		/// Return byte size of integer. 
		/// </summary>
		public static int IntToVarBytes(uint v, byte[] buf, int offset)
		{
			int i = offset;

			while (i < buf.Length && v >= 0x80)
			{
				buf[i++] = (byte)(v | 0x80);
				v >>= 7;
			}

			if (i < buf.Length)
			{
				buf[i++] = (byte)v;
				return i - offset;
			}
			return 0;
		}

		/// <summary>
		/// Decode an integer in variable 7-bit format.
		/// The high bit indicates if more bytes are used.
		/// Return value and byte size in array.
		/// </summary>
		public static int VarBytesToInt(byte[] buf, int offset, out int size)
		{
			int i = offset;
			int val = 0;
			int shift = 0;
			byte b;

			do
			{
				b = buf[i++];
				val |= (b & 0x7F) << shift;
				shift += 7;
			} while ((b & 0x80) != 0);

			size = i - offset;
			return val;
		}
	}
}
