/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	/// Map policy directives when creating a map and writing map items.
	/// </summary>
	public sealed class MapPolicy
	{
		/// <summary>
		/// Default unordered unique key map with normal put semantics.
		/// </summary>
		public static readonly MapPolicy Default = new MapPolicy();

		internal readonly int attributes;
		internal readonly int flags;
		internal readonly int itemCommand;
		internal readonly int itemsCommand;

		/// <summary>
		/// Default constructor.  
		/// Create unordered unique key map when map does not exist.
		/// Use normal update mode when writing map items.
		/// </summary>
		public MapPolicy()
			: this(MapOrder.UNORDERED, MapWriteMode.UPDATE)
		{
		}

		/// <summary>
		/// Create unique key map with specified order when map does not exist.
		/// Use specified write mode when writing map items.
		/// <para>
		/// This constructor should only be used for server versions &lt; 4.3.
		/// <seealso cref="MapPolicy(MapOrder,MapWriteFlags)"/> is recommended for server versions >= 4.3.
		/// </para>
		/// </summary>
		public MapPolicy(MapOrder order, MapWriteMode writeMode)
		{
			this.attributes = (int)order;
			this.flags = (int)MapWriteFlags.DEFAULT;

			switch (writeMode)
			{
				case MapWriteMode.UPDATE:
					itemCommand = MapOperation.PUT;
					itemsCommand = MapOperation.PUT_ITEMS;
					break;

				case MapWriteMode.UPDATE_ONLY:
					itemCommand = MapOperation.REPLACE;
					itemsCommand = MapOperation.REPLACE_ITEMS;
					break;

				case MapWriteMode.CREATE_ONLY:
					itemCommand = MapOperation.ADD;
					itemsCommand = MapOperation.ADD_ITEMS;
					break;
			}
		}

		/// <summary>
		/// Create unique key map with specified order when map does not exist.
		/// </summary>
		/// <param name="order">map order</param>
		/// <param name="flags">map write flags <see cref="MapWriteFlags"/></param>
		public MapPolicy(MapOrder order, MapWriteFlags flags)
		{
			this.attributes = (int)order;
			this.flags = (int)flags;
			this.itemCommand = MapOperation.PUT;
			this.itemsCommand = MapOperation.PUT_ITEMS;
		}

		/// <summary>
		/// Create unique key map with specified order and persist index flag when map does not exist.
		/// </summary>
		/// <param name="order">map order</param>
		/// <param name="flags">map write flags <see cref="MapWriteFlags"/></param>
		/// <param name="persistIndex">if true, persist map index. A map index improves lookup performance,
		///						but requires more storage.A map index can be created for a top-level
		///						ordered map only. Nested and unordered map indexes are not supported.</param>
		public MapPolicy(MapOrder order, MapWriteFlags flags, bool persistIndex)
		{
			int attr = (int)order;

			if (persistIndex)
			{
				attr |= 0x10;
			}

			this.attributes = attr;
			this.flags = (int)flags;
			this.itemCommand = MapOperation.PUT;
			this.itemsCommand = MapOperation.PUT_ITEMS;
		}
	}
}
