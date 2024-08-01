/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	/// Multi-record transaction (MRT) error status.
	/// </summary>
	public static class CommitError
	{
		public enum CommitErrorType
		{
			VERIFY_FAIL,
			VERIFY_FAIL_CLOSE_ABANDONED,
			VERIFY_FAIL_ABORT_ABANDONED,
			MARK_ROLL_FORWARD_ABANDONED,
			ROLL_FORWARD_ABANDONED,
			CLOSE_ABANDONED
		}

		public static string CommitErrorToString(CommitErrorType type)
		{
			return type switch
			{
				CommitErrorType.VERIFY_FAIL => "MRT verify failed. MRT aborted.",
				CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED => "MRT verify failed. MRT aborted. MRT client close abandoned. Server will eventually close the MRT.",
				CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED => "MRT verify failed. MRT client abort abandoned. Server will eventually abort the MRT.",
				CommitErrorType.MARK_ROLL_FORWARD_ABANDONED => "MRT client mark roll forward abandoned. Server will eventually abort the MRT.",
				CommitErrorType.ROLL_FORWARD_ABANDONED => "MRT client roll forward abandoned. Server will eventually commit the MRT.",
				CommitErrorType.CLOSE_ABANDONED => "MRT has been rolled forward, but MRT client close was abandoned. Server will eventually close the MRT.",
				_ => "Unexpected CommitErrorType"
			};
		}
	}
}
