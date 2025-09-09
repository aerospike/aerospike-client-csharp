/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	/// Asynchronous result notifications for batch get commands with variable bins per key.
	/// The result is sent in a single list.
	/// </summary>
	public interface BatchListListener
	{
		/// <summary>
		/// This method is called when the command completes successfully.
		/// </summary>
		/// <param name="records">
		/// record instances, <seealso cref="BatchRecord.record"/>
		///	will be null if the key is not found.
		///	</param>
		void OnSuccess(List<BatchRead> records);

		/// <summary>
		/// This method is called when the command fails.
		/// </summary>
		void OnFailure(AerospikeException ae);
	}
}
