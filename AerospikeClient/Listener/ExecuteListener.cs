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
	/// Asynchronous result notifications for execute commands.
	/// </summary>
	public interface ExecuteListener
	{
		/// <summary>
		/// This method is called when an asynchronous execute command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		/// <param name="obj">returned object</param>
		void OnSuccess(Key key, object obj);

		/// <summary>
		/// This method is called when an asynchronous execute command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}
