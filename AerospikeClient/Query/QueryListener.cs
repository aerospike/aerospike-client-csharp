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
	/// This method will be called for each record returned from a sync query. The user may throw a 
	/// <see cref="Aerospike.Client.AerospikeException.QueryTerminated"/> exception if the query
	/// should be aborted. If an exception is thrown, parallel query command threads to other nodes
	/// will also be terminated.
	/// </summary>
	/// <param name="key">unique record identifier</param>
	/// <param name="record">record instance</param>
	/// <exception cref="AerospikeException">if error occurs or query should be terminated</exception>
	public delegate void QueryListener(Key key, Record record);
}
