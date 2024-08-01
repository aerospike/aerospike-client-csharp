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
	/// Multi-record transaction (MRT) abort error status.
	/// </summary>
	public static class AbortError
	{
		public enum AbortErrorType
		{
			ROLL_BACK_ABANDONED,
			CLOSE_ABANDONED
		}

		public static string AbortErrorToString(AbortErrorType error)
		{
			return error switch
			{
				AbortErrorType.ROLL_BACK_ABANDONED => "MRT client roll back abandoned. Server will eventually abort the MRT.",
				AbortErrorType.CLOSE_ABANDONED => "MRT has been rolled back, but MRT client close was abandoned. Server will eventually close the MRT.",
				_ => "Unexpected AbortErrorType"
			};
		}
	}
}
