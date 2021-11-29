/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	/// Batch inline bit flags.
	/// </summary>
	[Flags]
	public enum InlineFlags
	{
		/// <summary>
		/// Allow batch to be processed immediately in the server's receiving thread for in-memory
		/// namespaces.
		/// <para>
		/// For batch transactions with smaller sized records (&lt;= 1K per record), inline
		/// processing will be significantly faster on in-memory namespaces.
		/// </para>
		/// </summary>
		INLINE_IN_MEMORY = 0x1,

		/// <summary>
		/// Allow batch to be processed immediately in the server's receiving thread for on-device
		/// namespaces (SSD, PMEM). Server versions &lt; 5.8 ignore this flag.
		/// </summary>
		INLINE_ON_DEVICE = 0x2
	}
}
