/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
		/// </summary>
		public MapPolicy(MapOrder order, MapWriteMode writeMode)
		{
			this.attributes = (int)order;

			switch (writeMode)
			{
				case MapWriteMode.UPDATE:
					itemCommand = MapBase.PUT;
					itemsCommand = MapBase.PUT_ITEMS;
					break;

				case MapWriteMode.UPDATE_ONLY:
					itemCommand = MapBase.REPLACE;
					itemsCommand = MapBase.REPLACE_ITEMS;
					break;

				case MapWriteMode.CREATE_ONLY:
					itemCommand = MapBase.ADD;
					itemsCommand = MapBase.ADD_ITEMS;
					break;
			}
		}
	}
}
