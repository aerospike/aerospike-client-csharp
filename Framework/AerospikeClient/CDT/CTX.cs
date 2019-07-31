/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
		/// Lookup map by value.
		/// </summary>
		public static CTX MapValue(Value key)
		{
			return new CTX(0x23, key);
		}

		internal int id;
		internal Value value;

		private CTX(int id, Value value)
		{
			this.id = id;
			this.value = value;
		}
	}
}
