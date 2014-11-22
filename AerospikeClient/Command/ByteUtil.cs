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
			switch (len)
			{
				case 0:
					return 0;

				case 1:
					return buf[offset];

				case 2:
				case 3:
				case 4:
					return BytesToIntegerObject(buf, offset, len);

				case 5:
				case 6:
				case 7:
				case 8:
					return BytesToLongObject(buf, offset, len);

				default:
					return BytesToBigInteger(buf, offset, len);
			}
		}

		public static object BytesToIntegerObject(byte[] buf, int offset, int len)
		{
			int val = 0;

			for (int i = 0; i < len; i++)
			{
				val <<= 8;
				val |= buf[offset + i];
			}
			return val;
		}

		public static object BytesToLongObject(byte[] buf, int offset, int len)
		{
			long val = 0;
		
			for (int i = 0; i < len; i++) {
				val <<= 8;
				val |= buf[offset+i];
			}
			return val;
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

		public static int LongToBytes(ulong v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			for (int i = 7; i >= 0; i--)
			{
				buf[offset + i] = (byte)(v & 0xff);
				v >>= 8;
			}
			return 8;
		}

		public static int LongToLittleBytes(ulong v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine. 
			for (int i = 0; i < 8; i++)
			{
				buf[offset + i] = (byte)(v & 0xff);
				v >>= 8;
			}
			return 8;
		}

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

		public static long BytesToLong(byte[] buf, int offset)
		{
			// Benchmarks show that BitConverter.ToInt64() conversion is slightly faster than a custom implementation.
			// This contradicts all other number conversion benchmarks.
			// Assume little endian machine and reverse contents.
			byte[] bytes = new byte[8];

			bytes[0] = buf[offset + 7];
			bytes[1] = buf[offset + 6];
			bytes[2] = buf[offset + 5];
			bytes[3] = buf[offset + 4];
			bytes[4] = buf[offset + 3];
			bytes[5] = buf[offset + 2];
			bytes[6] = buf[offset + 1];
			bytes[7] = buf[offset];

			return System.BitConverter.ToInt64(bytes, 0);
		}

		public static long LittleBytesToLong(byte[] buf, int offset)
		{
			// Benchmarks show that BitConverter.ToInt64() conversion is slightly faster than a custom implementation.
			// This contradicts all other number conversion benchmarks.
			// Assume little endian machine.
			return System.BitConverter.ToInt64(buf, offset);
		}
		
		public static int IntToBytes(uint v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			for (int i = 3; i >= 0; i--)
			{
				buf[offset + i] = (byte)(v & 0xff);
				v >>= 8;
			}
			return 4;
		}

		public static int IntToLittleBytes(uint v, byte[] buf, int offset)
		{
			for (int i = 0; i < 4; i++)
			{
				buf[offset + i] = (byte)(v & 0xff);
				v >>= 8;
			}
			return 4;
		}
		
		public static int BytesToInt(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt32().
			// Assume little endian machine and reverse/convert in one pass. 
			return (((buf[offset] & 0xFF) << 24) | 
				    ((buf[offset + 1] & 0xFF) << 16) | 
					((buf[offset + 2] & 0xFF) << 8) | 
					 (buf[offset + 3] & 0xFF));
		}

		public static int LittleBytesToInt(byte[] buf, int offset)
		{
			return ((buf[offset] & 0xFF) |
					((buf[offset + 1] & 0xFF) << 8) |
					((buf[offset + 2] & 0xFF) << 16) |
					((buf[offset + 3] & 0xFF) << 24));
		}
		
		public static int ShortToBytes(ushort v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset] = (byte)(v >> 8);
			buf[offset + 1] = (byte)(v & 0xFF);
			return 2;
		}

		public static int ShortToLittleBytes(ushort v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset] = (byte)(v & 0xFF);
			buf[offset + 1] = (byte)(v >> 8);
			return 2;
		}
		
		public static int BytesToShort(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt16().
			// Assume little endian machine and reverse/convert in one pass. 
			return ((buf[offset] & 0xFF) << 8) + (buf[offset + 1] & 0xFF);
		}

		public static int LittleBytesToShort(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt16().
			// Assume little endian machine and reverse/convert in one pass. 
			return (buf[offset] & 0xFF) + ((buf[offset + 1] & 0xFF) << 8);
		}
	
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
