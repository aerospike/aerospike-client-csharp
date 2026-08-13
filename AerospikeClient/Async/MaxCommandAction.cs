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
namespace Aerospike.Client
{
	/// <summary>
	/// How to handle cases when the asynchronous maximum number of concurrent database commands have been exceeded.
	/// </summary>
	public enum MaxCommandAction
	{
		/// <summary>
		/// Reject the command immediately with <see cref="AerospikeException.CommandRejected"/>.
		/// </summary>
		REJECT,

		/// <summary>
		/// Block the calling thread until a command slot becomes available.
		/// Avoid this action on threads that must remain responsive.
		/// </summary>
		BLOCK,

		/// <summary>
		/// Queue the command until a slot becomes available without blocking the calling thread.
		/// </summary>
		/// <remarks>
		/// The queue is bounded by <see cref="AsyncClientPolicy.asyncMaxCommandsInQueue"/>
		/// when that value is greater than zero.
		/// </remarks>
		DELAY,
	}
}
