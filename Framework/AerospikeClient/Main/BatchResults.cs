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
	/// Batch record results.
	/// </summary>
	public sealed class BatchResults
	{
		/// <summary>
		/// Record results.
		/// </summary>
		public readonly BatchRecord[] records;

		/// <summary>
		/// Indicates if all records returned success.
		/// </summary>
		public readonly bool status;

		/// <summary>
		/// Constructor.
		/// </summary>
		public BatchResults(BatchRecord[] records, bool status)
		{
			this.records = records;
			this.status = status;
		}
	}
}
