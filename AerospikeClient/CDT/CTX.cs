/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections;

namespace Aerospike.Client
{
	/// <summary>
	/// Nested CDT context.  Identifies the location of nested list/map to apply the operation.
	/// for the current level.  An array of CTX identifies location of the list/map on multiple
	/// levels on nesting.
	/// </summary>
	public sealed class CTX
	{
		/// <summary>
		/// Lookup list by index offset.
		/// <para>
		/// If the index is negative, the resolved index starts backwards from end of list.
		/// If an index is out of bounds, a parameter error will be returned.  Examples:
		/// <ul>
		/// <li>0: First item.</li>
		/// <li>4: Fifth item.</li>
		/// <li>-1: Last item.</li>
		/// <li>-3: Third to last item.</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static CTX ListIndex(int index)
		{
			return new CTX(0x10, Value.Get(index));
		}

		/// <summary>
		/// Lookup list by base list's index offset. If the list at index offset is not found,
		/// create it with the given sort order at that index offset. If pad is true and the
		/// index offset is greater than the bounds of the base list, nil entries will be
		/// inserted before the newly created list.
		/// </summary>
		public static CTX ListIndexCreate(int index, ListOrder order, bool pad)
		{
			return new CTX(0x10 | GetFlag(order, pad), Value.Get(index));
		}

		/// <summary>
		/// Lookup list by rank.
		/// <ul>
		/// <li>0 = smallest value</li>
		/// <li>N = Nth smallest value</li>
		/// <li>-1 = largest value</li>
		/// </ul>
		/// </summary>
		public static CTX ListRank(int rank)
		{
			return new CTX(0x11, Value.Get(rank));
		}

		/// <summary>
		/// Lookup list by value.
		/// </summary>
		public static CTX ListValue(Value key)
		{
			return new CTX(0x13, key);
		}

		/// <summary>
		/// Lookup map by index offset.
		/// <para>
		/// If the index is negative, the resolved index starts backwards from end of list.
		/// If an index is out of bounds, a parameter error will be returned.  Examples:
		/// <ul>
		/// <li>0: First item.</li>
		/// <li>4: Fifth item.</li>
		/// <li>-1: Last item.</li>
		/// <li>-3: Third to last item.</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static CTX MapIndex(int index)
		{
			return new CTX(0x20, Value.Get(index));
		}

		/// <summary>
		/// Lookup map by rank.
		/// <ul>
		/// <li>0 = smallest value</li>
		/// <li>N = Nth smallest value</li>
		/// <li>-1 = largest value</li>
		/// </ul>
		/// </summary>
		public static CTX MapRank(int rank)
		{
			return new CTX(0x21, Value.Get(rank));
		}

		/// <summary>
		/// Lookup map by key.
		/// </summary>
		public static CTX MapKey(Value key)
		{
			return new CTX(0x22, key);
		}

		/// <summary>
		/// Lookup map by base map's key. If the map at key is not found,
		/// create it with the given sort order at that key.
		/// </summary>
		public static CTX MapKeyCreate(Value key, MapOrder order)
		{
			return new CTX(0x22 | GetFlag(order), key);
		}

		/// <summary>
		/// Lookup map by value.
		/// </summary>
		public static CTX MapValue(Value key)
		{
			return new CTX(0x23, key);
		}

		/// <summary>
		/// Serialize context array to bytes.
		/// </summary>
		public static byte[] ToBytes(CTX[] ctx)
		{
			return PackUtil.Pack(ctx);
		}

		/// <summary>
		/// Deserialize bytes to context array.
		/// </summary>
		public static CTX[] FromBytes(byte[] bytes)
		{
			Unpacker unpacker = new Unpacker(bytes, 0, bytes.Length, false);
			var list = (IList)unpacker.UnpackList();
			int max = list.Count;
			CTX[] ctx = new CTX[max / 2];
			int i = 0;
			int count = 0;

			while (i < max)
			{
				int id = (int)(long)list[i];

				if (++i >= max)
				{
					throw new AerospikeException.Parse("List count must be even");
				}

				var obj = list[i];
				Value val = Value.Get(obj);

				ctx[count++] = new CTX(id, val);
				i++;
			}
			return ctx;
		}

		/// <summary>
		/// Serialize context array to base64 encoded string.
		/// </summary>
		public static string ToBase64(CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ctx);
			return Convert.ToBase64String(bytes);
		}

		/// <summary>
		/// Deserialize base64 encoded string to context array.
		/// </summary>
		public static CTX[] FromBase64(string base64)
		{
			byte[] bytes = Convert.FromBase64String(base64);
			return FromBytes(bytes);
		}

		internal static int GetFlag(ListOrder order, bool pad)
		{
			return (order == ListOrder.ORDERED) ? 0xc0 : pad ? 0x80 : 0x40;
		}

		internal static int GetFlag(MapOrder order)
		{
			switch (order)
			{
				default:
				case MapOrder.UNORDERED:
					return 0x40;
				case MapOrder.KEY_ORDERED:
					return 0x80;
				case MapOrder.KEY_VALUE_ORDERED:
					return 0xc0;
			}
		}

		public readonly int id;
		public readonly Value value;

		private CTX(int id, Value value)
		{
			this.id = id;
			this.value = value;
		}
	}
}
