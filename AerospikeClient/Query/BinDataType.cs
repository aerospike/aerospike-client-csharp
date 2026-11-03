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
	/// Scalar bin data type of a Top-K order key, used to interpret the bin's value for
	/// comparison; see <see cref="Statement.SetOrderBy(string, BinDataType, Order)"/>.
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
		/// Byte array value, compared as unsigned bytes; a prefix ranks before the longer value.
		/// </summary>
		BYTES
	}
}
