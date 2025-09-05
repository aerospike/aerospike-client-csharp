/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
namespace Aerospike.Client
{
	/// <summary>
	/// Generate pseudo random numbers using xorshift128+ algorithm.
	/// This class is not thread-safe and should be instantiated once per thread.
	/// </summary>
	public sealed class RandomShift
	{
		[ThreadStatic]
		private static RandomShift RandomShiftLocal;

		/// <summary>
		/// Get thread local instance of RandomShift.  The instance does not need
		/// to be placed in IIS HttpContext because it will only be used immediately
		/// in the same thread as it was retrieved.  IIS will not have a chance
		/// to switch threads on this instance.
		/// </summary>
		public static RandomShift ThreadLocalInstance
		{
			get
			{
				if (RandomShiftLocal == null)
				{
					RandomShiftLocal = new RandomShift();
				}
				return RandomShiftLocal;
			}
		}

		private ulong seed0;
		private ulong seed1;

		/// <summary>
		/// Generate seeds using standard Random class.
		/// </summary>
		public RandomShift()
		{
			// Do not use Environment.TickCount for seed because it is often
			// the same across concurrent threads, thus causing duplicate values.
			Random random = new Random(Guid.NewGuid().GetHashCode());
			byte[] buffer = new byte[sizeof(UInt64) * 2];
			random.NextBytes(buffer);
			seed0 = BitConverter.ToUInt64(buffer, 0);
			seed1 = BitConverter.ToUInt64(buffer, sizeof(UInt64));
		}

		/// <summary>
		/// Generate a random string of given size.
		/// </summary>
		public string NextString(int size)
		{
			char[] chars = new char[size];
			int r;

			for (int i = 0; i < size; i++)
			{
				r = Next(0, 62); // Lower alpha + Upper alpha + digit

				if (r < 26)
				{
					// Lower case
					chars[i] = (char)(r + 97);
				}
				else if (r < 52)
				{
					// Upper case
					chars[i] = (char)(r - 26 + 65);
				}
				else
				{
					// Digit
					chars[i] = (char)(r - 52 + 48);
				}
			}
			return new string(chars);
		}

		/// <summary>
		/// Generate random bytes.
		/// </summary>
		public void NextBytes(byte[] bytes)
		{
			int len = bytes.Length;
			int i = 0;

			while (i < len)
			{
				ulong r = NextLong();
				int n = Math.Min(len - i, 8);

				for (int j = 0; j < n; j++)
				{
					bytes[i++] = (byte)r;
					r >>= 8;
				}
			}
		}

		/// <summary>
		/// Generate random integer between begin (inclusive) and end (exclusive).
		/// </summary>
		public int Next(int begin, int end)
		{
			ulong b = (ulong)begin;
			ulong e = (ulong)end;
			return (int)((NextLong() % (e - b)) + b);
		}

		/// <summary>
		/// Generate random unsigned integer.
		/// </summary>
		public uint Next()
		{
			return (uint)NextLong();
		}

		/// <summary>
		/// Generate random unsigned long value.
		/// </summary>
		public ulong NextLong()
		{
			ulong s1 = seed0;
			ulong s0 = seed1;
			seed0 = s0;
			s1 ^= s1 << 23;
			seed1 = (s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5));
			return seed1 + s0;
		}
	}
}
