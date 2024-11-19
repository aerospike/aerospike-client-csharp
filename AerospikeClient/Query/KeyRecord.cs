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
	/// Container object for key identifier and record data.
	/// </summary>
	public class KeyRecord
	{
		/// <summary>
		/// Unique identifier for record.
		/// </summary>
		public Key Key { get; private set; }

		/// <summary>
		/// Record header and bin data.
		/// </summary>
		public Record Record { get; set; }

		/// <summary>
		/// Initialize key and record.
		/// </summary>
		public KeyRecord(Key key, Record record)
		{
			this.Key = key;
			this.Record = record;
		}
	}
}
