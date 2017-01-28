/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
	/// Map storage order.
	/// </summary>
	public enum MapOrder
	{
		/// <summary>
		/// Map is not ordered.  This is the default.
		/// </summary>
		UNORDERED = 0,

		/// <summary>
		/// Order map by key.
		/// </summary>
		KEY_ORDERED = 1,

		/// <summary>
		/// Order map by key, then value.
		/// </summary>
		KEY_VALUE_ORDERED = 3
	}
}
