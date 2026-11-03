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
		internal readonly byte[] digest;
		/// <summary>
		/// Server last-update time from Field 14, or zero when the row carried none
		/// (client-side reduce / older servers).
		/// </summary>
		internal readonly long lastUpdateTime;
		/// <summary>Optional secondary (then-order-by) key; compared only when the primary ties.</summary>
		internal readonly OrderKey secondary;

		internal OrderKey(Record record, string bin, BinDataType type, Order order, byte[] digest)
			: this(record, bin, type, order, digest, null, 0)
		{
		}

		internal OrderKey(
			Record record,
			string bin,
			BinDataType type,
			Order order,
			byte[] digest,
			OrderKey secondary)
			: this(record, bin, type, order, digest, secondary, 0)
		{
		}

		internal OrderKey(
			Record record,
			string bin,
			BinDataType type,
			Order order,
			byte[] digest,
			OrderKey secondary,
			long lastUpdateTime)
		{
			this.value = ReadValue(record, bin, type);
			this.type = type;
			this.order = order;
			this.digest = digest;
			this.secondary = secondary;
			this.lastUpdateTime = lastUpdateTime;
		}

		internal bool IsNil => value == null;

		public int CompareTo(OrderKey other)
		{
			int cmp = ComparePrimary(other);

			if (cmp == 0 && secondary != null && other.secondary != null)
			{
				cmp = secondary.ComparePrimary(other.secondary);
			}

			if (cmp != 0)
			{
				return cmp;
			}
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
				case BinDataType.BYTES:
					return value is byte[] ? value : null;
				default:
					throw new AerospikeException(ResultCode.PARSE_ERROR, "Unsupported BinDataType: " + type);
			}
		}

		private static int CompareValues(OrderKey a, OrderKey b)
		{
			if (a.type == BinDataType.BYTES)
			{
				return CompareBytes((byte[])a.value, (byte[])b.value);
			}

			if (a.type == BinDataType.DOUBLE)
			{
				// NaN ranks after every finite value (and +Inf); −0.0 equals 0.0
				// (tie breaks by digest in CompareTo).
				return CompareDouble((double)a.value, (double)b.value);
			}

			return ((IComparable)a.value).CompareTo(b.value);
		}

		private static int CompareDouble(double a, double b)
		{
			if (a < b)
			{
				return -1;
			}

			if (a > b)
			{
				return 1;
			}

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

			// Collapse NaN payloads to a canonical quiet NaN so NaN ranks above +Inf.
			if (((ulong)bits & 0x7ff0000000000000UL) == 0x7ff0000000000000UL &&
				((ulong)bits & 0x000fffffffffffffUL) != 0UL)
			{
				return 0x7ff8000000000000L;
			}

			return bits;
		}

		/// <summary>
		/// Unsigned byte order; a prefix ranks before the longer value.
		/// </summary>
		private static int CompareBytes(byte[] a, byte[] b)
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
