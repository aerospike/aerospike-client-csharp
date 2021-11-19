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
	/// Asynchronous result notifications for batch operate commands.
	/// </summary>
	public interface BatchRecordArrayListener
	{
		/// <summary>
		/// This method is called when the command completes successfully.
		/// The returned record array is in positional order with the original key array order.
		/// </summary>
		/// <param name="records">record instances, always populated.</param>
		/// <param name="status">true if all records returned success.</param>
		void OnSuccess(BatchRecord[] records, bool status);

		/// <summary>
		/// This method is called when one or more keys fail.
		/// </summary>
		/// <param name="records">
		/// record instances, always populated. <see cref="Aerospike.Client.BatchRecord.resultCode"/>
		/// indicates if an error occurred for each record instance.
		/// </param>
		/// <param name="ae">error that occurred</param>
		void OnFailure(BatchRecord[] records, AerospikeException ae);
	}
}
