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
	/// Map return type. Type of data to return when selecting or removing items from the map.
	/// </summary>
	[Flags]
	public enum MapReturnType
	{
		/// <summary>
		/// Do not return a result.
		/// </summary>
		NONE = 0,

		/// <summary>
		/// Return key index order.
		/// <ul>
		/// <li>0 = first key</li>
		/// <li>N = Nth key</li>
		/// <li>-1 = last key</li>
		/// </ul>
		/// </summary>
		INDEX = 1,

		/// <summary>
		/// Return reverse key order.
		/// <ul>
		/// <li>0 = last key</li>
		/// <li>-1 = first key</li>
		/// </ul>
		/// </summary>
		REVERSE_INDEX = 2,

		/// <summary>
		/// Return value order.
		/// <ul>
		/// <li>0 = smallest value</li>
		/// <li>N = Nth smallest value</li>
		/// <li>-1 = largest value</li>
		/// </ul>
		/// </summary>
		RANK = 3,

		/// <summary>
		/// Return reverse value order.
		/// <ul>
		/// <li>0 = largest value</li>
		/// <li>N = Nth largest value</li>
		/// <li>-1 = smallest value</li>
		/// </ul>
		/// </summary>
		REVERSE_RANK = 4,

		/// <summary>
		/// Return count of items selected.
		/// </summary>
		COUNT = 5,

		/// <summary>
		/// Return key for single key read and key list for range read.
		/// </summary>
		KEY = 6,

		/// <summary>
		/// Return value for single key read and value list for range read.
		/// </summary>
		VALUE = 7,

		/// <summary>
		/// Return key/value items. The possible return types are:
		/// <ul>
		/// <li>HashMap : Returned for unordered maps</li>
		/// <li>TreeMap : Returned for key ordered maps</li>
		/// <li>List&lt;Entry&gt; : Returned for range results where range order needs to be preserved.</li>
		/// </ul>
		/// </summary>
		KEY_VALUE = 8,

		/// <summary>
		/// Return true if count > 0.
		/// </summary>
		EXISTS = 13,

		/// <summary>
		/// Return an unordered map.
		/// </summary>
		UNORDERED_MAP = 16,

		/// <summary>
		/// Return an ordered map.
		/// </summary>
		ORDERED_MAP = 17,

		/// <summary>
		/// Invert meaning of map command and return values.  For example:
		/// <code>
		/// MapOperation.RemoveByKeyRange(binName, keyBegin, keyEnd, MapReturnType.KEY | MapReturnType.INVERTED);
		/// </code>
		/// With the INVERTED flag enabled, the keys outside of the specified key range will be removed and returned.
		/// </summary>
		INVERTED = 0x10000
	}
}
