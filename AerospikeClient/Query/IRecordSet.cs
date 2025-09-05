/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	public interface IRecordSet
	{
		/// <summary>
		/// Retrieve next record. Returns true if record exists and false if no more 
		/// records are available.
		/// This method will block until a record is retrieved or the query is cancelled.
		/// </summary>
		bool Next();

		/// <summary>
		/// Close query.
		/// </summary>
		void Dispose();

		/// <summary>
		/// Close query.
		/// </summary>
		void Close();

		/// <summary>
		/// Get record's unique identifier.
		/// </summary>
		Key Key { get; }

		/// <summary>
		/// Get record's header and bin data.
		/// </summary>
		Record Record { get; }

		/// <summary>
		/// Get CancellationToken associated with this query.
		/// </summary>
		CancellationToken CancelToken { get; }

		/// <summary>
		/// Put a record on the queue.
		/// </summary>
		bool Put(KeyRecord record);
	}
}
