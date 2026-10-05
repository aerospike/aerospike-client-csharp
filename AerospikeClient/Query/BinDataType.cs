/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
	/// Scalar bin data type used by <see cref="Reduce"/> order/reduce specs to interpret a bin's
	/// value for comparison (min/max/order-by).
	/// </summary>
	public enum BinDataType
	{
		/// <summary>
		/// Integer (long) value.
		/// </summary>
		INTEGER,

		/// <summary>
		/// Double precision floating point value.
		/// </summary>
		DOUBLE,

		/// <summary>
		/// String value.
		/// </summary>
		STRING,

		/// <summary>
		/// Byte array value.
		/// </summary>
		BYTES
	}
}
