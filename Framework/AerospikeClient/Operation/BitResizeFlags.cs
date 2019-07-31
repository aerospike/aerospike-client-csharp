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
	/// Bitwise operation flags for resize.
	/// </summary>
	public enum BitResizeFlags
	{
		/// <summary>
		/// Default.  Pad end of bitmap.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// Add/remove bytes from the beginning instead of the end.
		/// </summary>
		FROM_FRONT = 1,

		/// <summary>
		/// Only allow the byte[] size to increase.
		/// </summary>
		GROW_ONLY = 2,

		/// <summary>
		/// Only allow the byte[] size to decrease.
		/// </summary>
		SHRINK_ONLY = 4
	}
}
