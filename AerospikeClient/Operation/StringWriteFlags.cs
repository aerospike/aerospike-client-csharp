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
	/// String operation policy write bit flags. Use BITWISE OR to combine flags.
	/// </summary>
	/// <example>
	/// <code>
	/// StringWriteFlags flags = StringWriteFlags.NO_FAIL;
	/// </code>
	/// </example>
	[Flags]
	public enum StringWriteFlags
	{
		/// <summary>
		/// Default. Allow create or update.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// Update existing values only.
		/// </summary>
		UPDATE_ONLY = 2,

		/// <summary>
		/// Do not raise error if operation cannot be applied to the bin
		/// (e.g. wrong bin type). The bin is left unchanged and a null
		/// result is returned for that operation.
		/// </summary>
		NO_FAIL = 4
	}
}
