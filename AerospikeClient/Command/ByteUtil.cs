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
using System.IO;
using System.Text;
using System.Numerics;
using System.Runtime.Serialization;

namespace Aerospike.Client
{
	public sealed class ByteUtil
	{
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
					MsgUnpacker unpacker = new MsgUnpacker(false);
					return unpacker.ParseList(buf, offset, len);
				}
				case ParticleType.MAP:
				{
					MsgUnpacker unpacker = new MsgUnpacker(false);
					return unpacker.ParseMap(buf, offset, len);
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

			return System.BitConverter.ToInt64(bytes, offset);
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

		public static int BytesToInt(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt32().
			// Assume little endian machine and reverse/convert in one pass. 
			return (((buf[offset] & 0xFF) << 24) | 
				    ((buf[offset + 1] & 0xFF) << 16) | 
					((buf[offset + 2] & 0xFF) << 8) | 
					 (buf[offset + 3] & 0xFF));
		}

		public static int ShortToBytes(ushort v, byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.GetBytes().
			// Assume little endian machine and reverse/convert in one pass. 
			buf[offset] = (byte)(v >> 8);
			buf[offset + 1] = (byte)(v & 0xFF);
			return 2;
		}

		public static int BytesToShort(byte[] buf, int offset)
		{
			// Benchmarks show that custom conversion is faster than System.BitConverter.ToInt16().
			// Assume little endian machine and reverse/convert in one pass. 
			return ((buf[offset] & 0xFF) << 8) + (buf[offset + 1] & 0xFF);
		}
	}
}
