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
		/// <summary>Optional secondary (then-order-by) key; compared only when the primary ties.</summary>
		internal readonly OrderKey secondary;
		/// <summary>UTF-8 bytes for STRING order keys; null otherwise.</summary>
		private readonly byte[] stringBytes;

		internal OrderKey(Record record, string bin, BinDataType type, Order order, OrderByFlags flags, byte[] digest)
			: this(record, bin, type, order, flags, digest, null)
		{
		}

		internal OrderKey(
			Record record,
			string bin,
			BinDataType type,
			Order order,
			OrderByFlags flags,
			byte[] digest,
			OrderKey secondary)
		{
			object raw = ReadValue(record, bin, type);
			this.type = type;
			this.order = order;
			this.flags = flags;
			this.digest = digest;
			this.secondary = secondary;

			// STRING order keys compare as UTF-8 bytes (current server Top-K behavior).
			// Unicode collation (R73) is still an open design choice — do not invent a client
			// collator here until that lands (sort keys or an agreed comparator).
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
			int cmp = ComparePrimary(other);

			if (cmp != 0)
			{
				return cmp;
			}

			if (secondary != null && other.secondary != null)
			{
				cmp = secondary.ComparePrimary(other.secondary);

				if (cmp != 0)
				{
					return cmp;
				}
			}

			// Tie-break: digest ascending (stable, deterministic ordering).
			return CompareDigest(digest, other.digest);
		}

		/// <summary>
		/// Compare this key's primary value only (nil last; no digest tie-break).
		/// </summary>
		private int ComparePrimary(OrderKey other)
		{
			bool nilA = IsNil;
			bool nilB = other.IsNil;

			if (nilA || nilB)
			{
				if (nilA && nilB)
				{
					return 0;
				}
				// NIL sorts last in either direction.
				return nilA ? 1 : -1;
			}

			int cmp = CompareValues(this, other);

			if (cmp != 0)
			{
				return order == Order.ASC ? cmp : -cmp;
			}
			return 0;
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
					// Server always returns integer bins as long; accept int for hand-built records.
					return value switch
					{
						long l => l,
						int i => (long)i,
						_ => null
					};
				case BinDataType.DOUBLE:
					return value is double ? value : null;
				case BinDataType.STRING:
					return value is string ? value : null;
				case BinDataType.BYTES:
					return value is byte[] ? value : null;
				default:
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Unsupported BinDataType: " + type);
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
				// Server / PRD R29: NaN ranks after every finite value (and +Inf);
				// −0.0 equals 0.0 (tie breaks by digest in CompareTo).
				return CompareDouble((double)a.value, (double)b.value);
			}

			return ((IComparable)a.value).CompareTo(b.value);
		}

		private static int CompareDouble(double a, double b)
		{
			// Finite ordering, including −0.0 == 0.0 (IEEE equality).
			if (a < b)
			{
				return -1;
			}

			if (a > b)
			{
				return 1;
			}

			// a and b are equal, or at least one is NaN (NaN compares unequal to everything).
			if (!double.IsNaN(a) && !double.IsNaN(b))
			{
				return 0;
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

		private static int CompareBytes(byte[] a, byte[] b) =>
			CompareBytes(a, b, false);

		/// <summary>
		/// Unsigned byte order; optional ASCII A–Z fold to match current server Top-K.
		/// </summary>
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
