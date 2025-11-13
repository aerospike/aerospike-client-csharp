/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	/// Flags that control what data is selected and returned by path expression operations.
	/// These flags can be combined using bitwise OR operations.
	/// </summary>
	[Flags]
	public enum SelectFlags
	{
		/// <summary>
		/// Return the matching data tree structure.
		/// This preserves the original data structure hierarchy in the result.
		/// </summary>
		MATCHING_TREE = 0,

		/// <summary>
		/// Return only the values from list operations.
		/// When used with list or map operations, returns a list of matching values instead of list items or key-value pairs.
		/// </summary>
		LEAF_VALUE = 1,

		/// <summary>
		/// Return only the keys from map operations.
		/// When used with map operations, returns a list of matching keys instead of key-value pairs.
		/// </summary>
		MAP_KEYS = 2,

		/// <summary>
		/// Apply the operation to the matching data.
		/// </summary>
		APPLY = 4,

		/// <summary>
		/// Do not fail the operation if no matches are found.
		/// Returns empty result instead of failing when no data matches the criteria.
		/// </summary>
		NO_FAIL = 0x10
	}
}
