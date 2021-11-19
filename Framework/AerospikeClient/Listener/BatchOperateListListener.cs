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
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous result notifications for batch operate commands with variable operations.
	/// </summary>
	public interface BatchOperateListListener
	{
		/// <summary>
		/// This method is called when the command completes successfully.
		/// </summary>
		/// <param name="records">
		/// record instances, <see cref="Aerospike.Client.BatchRecord.record"/>
		///	will be null if an error occurred for that key.
		///	</param>
		/// <param name="status">true if all records returned success.</param>
		void OnSuccess(List<BatchRecord> records, bool status);

		/// <summary>
		/// This method is called when the command fails.
		/// </summary>
		void OnFailure(AerospikeException ae);
	}
}
