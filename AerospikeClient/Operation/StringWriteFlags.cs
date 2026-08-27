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
		/// Create the bin only if it does not already exist. Valid on the eight additive
		/// create-ops: insert, overwrite, concat, append, prepend, padStart, padEnd, and
		/// repeat. Mutually exclusive with <see cref="UPDATE_ONLY"/> and invalid with a
		/// <see cref="CTX"/> path.
		/// </summary>
		CREATE_ONLY = 1,

		/// <summary>
		/// Update existing values only.
		/// </summary>
		UPDATE_ONLY = 2,

		/// <summary>
		/// Do not raise an error when an in-op execution failure would otherwise
		/// abort the modify. The bin keeps its unmodified value and the operation
		/// result is that source string — not null. Does not suppress wrong-type or
		/// invalid-UTF-8 errors, and does not suppress flag validation failures such
		/// as mutually exclusive <see cref="CREATE_ONLY"/> and <see cref="UPDATE_ONLY"/>.
		/// </summary>
		NO_FAIL = 4
	}
}
