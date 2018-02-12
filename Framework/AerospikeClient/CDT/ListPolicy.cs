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
	/// List policy directives when creating a list and writing list items.
	/// </summary>
	public sealed class ListPolicy
	{
		/// <summary>
		/// Default unordered list with normal write semantics.
		/// </summary>
		public static readonly ListPolicy Default = new ListPolicy();

		internal readonly int attributes;
		internal readonly int flags;

		/// <summary>
		/// Default constructor.  
		/// Create unordered list when list does not exist.
		/// Use normal update mode when writing list items.
		/// </summary>
		public ListPolicy()
			: this(ListOrder.UNORDERED, ListWriteFlags.DEFAULT)
		{
		}

		/// <summary>
		/// Create list with specified order when list does not exist.
		/// Use specified write flags when writing list items.
		/// </summary>
		public ListPolicy(ListOrder order, ListWriteFlags flags)
		{
			this.attributes = (int)order;
			this.flags = (int)flags;
		}
	}
}
