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
namespace Aerospike.Client
{
	/// <summary>
	/// Flags that control what data is selected and returned by path expression operations.
	/// These flags can be combined using bitwise OR operations.
	/// </summary>
	[Flags]
	public enum SelectFlag
	{
		/// <summary>
		/// Return the matching data tree structure.
		/// This preserves the original data structure hierarchy in the result.
		/// </summary>
		MATCHING_TREE = 0,

		/// <summary>
		/// Return the list of the values of the nodes finally selected by the context.
		/// For maps, this returns the value of each (key, value) pair.
		/// </summary>
		VALUE = 1,

		/// <summary>
		/// Return the list of the values of the nodes finally selected by the context.
		/// This is a synonym for VALUE to make it clear in your
		/// source code that you're expecting a list.
		/// </summary>
		LIST_VALUE = VALUE,

		/// <summary>
		/// Return the list of map values of the nodes finally selected by the context.
		/// This is a synonym for VALUE to make it clear in your
		/// source code that you're expecting a map.  See also MAP_KEY_VALUE.
		/// </summary>
		MAP_VALUE = VALUE,

		/// <summary>
		/// Return only the keys from map operations.
		/// When used with map operations, returns a list of matching keys instead of key-value pairs.
		/// </summary>
		MAP_KEY = 2,

		/// <summary>
		/// Returns the list of map (key, value) pairs of the nodes finally selected
		///  by the context.  This is a synonym for setting both
		/// MAP_KEY and MAP_VALUE bits together.
		/// </summary>
		MAP_KEY_VALUE = MAP_KEY | MAP_VALUE,

		/// <summary>
		/// If the expression in the context hits an invalid type (e.g., selects
		/// as an integer when the value is a string), do not fail the operation;
		/// just ignore those elements.  Interpret UNKNOWN as false instead.
		/// </summary>
		NO_FAIL = 0x10,
	}
}
