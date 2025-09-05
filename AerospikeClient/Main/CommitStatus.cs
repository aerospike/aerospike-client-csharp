/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	/// Transaction commit status code.
	/// </summary>
	public static class CommitStatus
	{
		public enum CommitStatusType
		{
			OK,
			ALREADY_COMMITTED,
			ROLL_FORWARD_ABANDONED,
			CLOSE_ABANDONED
		}

		public static string CommitErrorToString(CommitStatusType status)
		{
			return status switch
			{
				CommitStatusType.OK => "Commit succeeded.",
				CommitStatusType.ALREADY_COMMITTED => "Already committed.",
				CommitStatusType.ROLL_FORWARD_ABANDONED => "Transaction client roll forward abandoned. Server will eventually commit the transaction.",
				CommitStatusType.CLOSE_ABANDONED => "Transaction has been rolled forward, but transaction client close was abandoned. Server will eventually close the transaction.",
				_ => "Unexpected AbortStatusType."
			};
		}
	}
}
