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
namespace Aerospike.Client
{
	/// <summary>
	/// Expression write flags.
	/// </summary>
	[Flags]
	public enum ExpWriteFlags
	{
		/// <summary>
		/// Default. Allow create or update.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// If bin does not exist, a new bin will be created.
		/// If bin exists, the operation will be denied.
		/// If bin exists, fail with <see cref="Aerospike.Client.ResultCode.BIN_EXISTS_ERROR"/>
		/// when <see cref="POLICY_NO_FAIL"/> is not set.
		/// </summary>
		CREATE_ONLY = 1,

		/// <summary>
		/// If bin exists, the bin will be overwritten.
		/// If bin does not exist, the operation will be denied.
		/// If bin does not exist, fail with <see cref="Aerospike.Client.ResultCode.BIN_NOT_FOUND"/>
		/// when <see cref="POLICY_NO_FAIL"/> is not set.
		/// </summary>
		UPDATE_ONLY = 2,

		/// <summary>
		/// If expression results in nil value, then delete the bin. Otherwise, fail with
		/// <see cref="Aerospike.Client.ResultCode.OP_NOT_APPLICABLE"/>
		/// when <see cref="POLICY_NO_FAIL"/> is not set.
		/// </summary>
		ALLOW_DELETE = 4,

		/// <summary>
		/// Do not raise error if operation is denied.
		/// </summary>
		POLICY_NO_FAIL = 8,

		/// <summary>
		/// Ignore failures caused by the expression resolving to unknown or a non-bin type.
		/// </summary>
		EVAL_NO_FAIL = 16
	}
}
