/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Sort key used internally by <see cref="TopKReduceSpec"/>.
	/// <para>
	/// Missing, mismatched, and collection values are NIL and sort last.
	/// </para>
	/// </summary>
	internal sealed class OrderKey : IComparable<OrderKey>
	{
		internal readonly object value;
		internal readonly BinDataType type;
		internal readonly Order order;
		internal readonly OrderByFlags flags;
		internal readonly byte[] digest;
		/// <summary>UTF-8 bytes for STRING order keys; null otherwise.</summary>
		private readonly byte[] stringBytes;

		internal OrderKey(Record record, string bin, BinDataType type, Order order, OrderByFlags flags, byte[] digest)
		{
			object raw = ReadValue(record, bin, type);
			this.type = type;
			this.order = order;
			this.flags = flags;
			this.digest = digest;

			if (raw is string s)
			{
				this.stringBytes = Encoding.UTF8.GetBytes(s);
				this.value = raw;
			}
			else
			{
				this.stringBytes = null;
				this.value = raw;
			}
		}

		internal bool IsNil => value == null;

		public int CompareTo(OrderKey other)
		{
			bool nilA = IsNil;
			bool nilB = other.IsNil;

			if (nilA || nilB)
			{
				if (nilA && nilB)
				{
					// Break NIL ties by digest.
					return CompareDigest(digest, other.digest);
				}
				// NIL sorts last in either direction.
				return nilA ? 1 : -1;
			}

			int cmp = CompareValues(this, other);

			if (cmp != 0)
			{
				return order == Order.ASC ? cmp : -cmp;
			}
			// Tie-break: digest ascending (stable, deterministic ordering).
			return CompareDigest(digest, other.digest);
		}

		/// <summary>
		/// Extract the scalar order-by value, or null for NIL.
		/// </summary>
		internal static object ReadValue(Record record, string bin, BinDataType type)
		{
			object value = record.GetValue(bin);

			// byte[] implements IList in .NET; exclude it so BYTES order-by works.
			if (value == null || value is IDictionary || (value is IList && value is not byte[]))
			{
				return null;
			}

			switch (type)
			{
				case BinDataType.INTEGER:
					// Server always returns integer bins as long.
					return value is long ? value : null;
				case BinDataType.DOUBLE:
					return value is double ? value : null;
				case BinDataType.STRING:
					return value is string ? value : null;
				case BinDataType.BYTES:
					return value is byte[] ? value : null;
				default:
					throw new ArgumentException("Unsupported BinDataType: " + type);
			}
		}

		private static int CompareValues(OrderKey a, OrderKey b)
		{
			if (a.type == BinDataType.STRING)
			{
				return CompareBytes(a.stringBytes, b.stringBytes, a.flags == OrderByFlags.CASE_INSENSITIVE);
			}

			if (a.type == BinDataType.BYTES)
			{
				return CompareBytes((byte[])a.value, (byte[])b.value);
			}

			if (a.type == BinDataType.DOUBLE)
			{
				// Match Java Double.compare: NaN sorts after all finite values; -0.0 < +0.0.
				return CompareDouble((double)a.value, (double)b.value);
			}

			return ((IComparable)a.value).CompareTo(b.value);
		}

		private static int CompareDouble(double a, double b)
		{
			// Match Java Double.compare / Double.doubleToLongBits:
			// NaN sorts after all finite values (and +Inf); -0.0 < +0.0.
			if (a < b)
			{
				return -1;
			}

			if (a > b)
			{
				return 1;
			}

			long ba = DoubleToComparableBits(a);
			long bb = DoubleToComparableBits(b);

			if (ba == bb)
			{
				return 0;
			}

			return ba < bb ? -1 : 1;
		}

		private static long DoubleToComparableBits(double value)
		{
			long bits = BitConverter.DoubleToInt64Bits(value);

			// Collapse NaN payloads to Java's canonical quiet NaN so NaN ranks above +Inf.
			if (((ulong)bits & 0x7ff0000000000000UL) == 0x7ff0000000000000UL &&
				((ulong)bits & 0x000fffffffffffffUL) != 0UL)
			{
				return 0x7ff8000000000000L;
			}

			return bits;
		}

		private static int CompareBytes(byte[] a, byte[] b)
		{
			return CompareBytes(a, b, false);
		}

		private static int CompareBytes(byte[] a, byte[] b, bool foldAscii)
		{
			int len = Math.Min(a.Length, b.Length);

			for (int i = 0; i < len; i++)
			{
				int ai = a[i] & 0xff;
				int bi = b[i] & 0xff;

				if (foldAscii)
				{
					if (ai >= 'A' && ai <= 'Z')
					{
						ai += 'a' - 'A';
					}

					if (bi >= 'A' && bi <= 'Z')
					{
						bi += 'a' - 'A';
					}
				}

				int cmp = ai - bi;

				if (cmp != 0)
				{
					return cmp;
				}
			}
			return a.Length - b.Length;
		}

		private static int CompareDigest(byte[] a, byte[] b)
		{
			int len = Math.Min(a.Length, b.Length);

			for (int i = 0; i < len; i++)
			{
				int cmp = (a[i] & 0xff) - (b[i] & 0xff);

				if (cmp != 0)
				{
					return cmp;
				}
			}
			return a.Length - b.Length;
		}
	}
}
