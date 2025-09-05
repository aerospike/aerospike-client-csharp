/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	/// List write bit flags.
	/// </summary>
	[Flags]
	public enum ListWriteFlags
	{
		/// <summary>
		/// Default.  Allow duplicate values and insertions at any index.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// Only add unique values.
		/// </summary>
		ADD_UNIQUE = 1,

		/// <summary>
		/// Enforce list boundaries when inserting.  Do not allow values to be inserted
		/// at index outside current list boundaries.
		/// </summary>
		INSERT_BOUNDED = 2,

		/// <summary>
		/// Do not raise error if a list item fails due to write flag constraints.
		/// </summary>
		NO_FAIL = 4,

		/// <summary>
		/// Allow other valid list items to be committed if a list item fails due to
		/// write flag constraints.
		/// </summary>
		PARTIAL = 8
	}
}
