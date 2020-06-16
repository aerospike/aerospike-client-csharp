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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Map write bit flags. 
	/// Requires server versions >= 4.3.
	/// </summary>
	[Flags]
	public enum MapWriteFlags
	{
		/// <summary>
		/// Default.  Allow create or update.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// If the key already exists, the item will be denied.
		/// If the key does not exist, a new item will be created.
		/// </summary>
		CREATE_ONLY = 1,

		/// <summary>
		/// If the key already exists, the item will be overwritten.
		/// If the key does not exist, the item will be denied.
		/// </summary>
		UPDATE_ONLY = 2,

		/// <summary>
		/// Do not raise error if a map item is denied due to write flag constraints.
		/// </summary>
		NO_FAIL = 4,

		/// <summary>
		/// Allow other valid map items to be committed if a map item is denied due to
		/// write flag constraints.
		/// </summary>
		PARTIAL = 8
	}
}
