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
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// Unique key map write type.
	/// This enum should only be used for server versions &lt; 4.3.
	/// <seealso cref="MapWriteFlags"/> are recommended for server versions >= 4.3.
	/// </summary>
	public enum MapWriteMode
	{
		/// <summary>
		/// If the key already exists, the item will be overwritten.
		/// If the key does not exist, a new item will be created.
		/// </summary>
		UPDATE,

		/// <summary>
		/// If the key already exists, the item will be overwritten.
		/// If the key does not exist, the write will fail.
		/// </summary>
		UPDATE_ONLY,

		/// <summary>
		/// If the key already exists, the write will fail.
		/// If the key does not exist, a new item will be created.
		/// </summary>
		CREATE_ONLY
	}
}
