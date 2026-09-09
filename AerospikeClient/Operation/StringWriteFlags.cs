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
	/// StringWriteFlags flags = StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL;
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
		/// Apply the operation only if the bin does not already exist. Against a live bin
		/// the server returns <see cref="ResultCode.BIN_EXISTS_ERROR"/>.
		/// <para>
		/// Valid only on the eight additive create-ops: insert, overwrite, concat, append,
		/// prepend, padStart, padEnd, and repeat. On any other string modify op the server
		/// rejects it with <see cref="ResultCode.PARAMETER_ERROR"/> via that op's flag mask.
		/// </para>
		/// <para>
		/// <see cref="CREATE_ONLY"/> combined with <see cref="UPDATE_ONLY"/> is
		/// <see cref="ResultCode.PARAMETER_ERROR"/>, and <see cref="CREATE_ONLY"/> on a
		/// <see cref="CTX"/> (nested) path is <see cref="ResultCode.PARAMETER_ERROR"/>. None of
		/// those three rejections is suppressible by <see cref="NO_FAIL"/>: the server raises
		/// them while parsing the operation's arguments, upstream of every NO_FAIL test.
		/// </para>
		/// </summary>
		CREATE_ONLY = 1,

		/// <summary>
		/// Apply the operation only to an existing bin, disabling bin creation. On a missing
		/// bin the operation is a silent no-op and the bin is not created. Valid on all string
		/// modify ops.
		/// <para>
		/// Mutually exclusive with <see cref="CREATE_ONLY"/>; combining the two is
		/// <see cref="ResultCode.PARAMETER_ERROR"/>.
		/// </para>
		/// </summary>
		UPDATE_ONLY = 2,

		/// <summary>
		/// Do not raise an error when the modify itself cannot be applied. The operation
		/// becomes a silent success and the bin is left at its unmodified prior value.
		/// <para>
		/// <see cref="NO_FAIL"/> does not suppress every failure. A wrong bin type
		/// (<see cref="ResultCode.BIN_TYPE_ERROR"/>) and invalid UTF-8 in the bin
		/// (<see cref="ResultCode.INVALID_ENCODING"/>) surface regardless of the flag, as do
		/// the argument-parsing rejections listed on <see cref="CREATE_ONLY"/>.
		/// </para>
		/// </summary>
		NO_FAIL = 4
	}
}
