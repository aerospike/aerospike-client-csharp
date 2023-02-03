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
	/// Asynchronous result notifications for batch exists commands.
	/// The result is sent in a single array.
	/// </summary>
	public interface ExistsArrayListener
	{
		/// <summary>
		/// This method is called when the command completes successfully.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="keys">unique record identifiers</param>
		/// <param name="exists">whether keys exists on server</param>
		void OnSuccess(Key[] keys, bool[] exists);

		/// <summary>
		/// This method is called when the command fails. The AerospikeException is likely to be
		/// <see cref="Aerospike.Client.AerospikeException.BatchExists"/> which contains results
		/// for keys that did complete.
		/// </summary>
		void OnFailure(AerospikeException ae);
	}
}
