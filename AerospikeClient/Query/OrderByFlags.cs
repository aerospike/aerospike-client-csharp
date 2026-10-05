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
	/// Options for <see cref="Reduce.OrderBy(string, BinDataType, Order, OrderByFlags)"/> and
	/// <see cref="Reduce.TopK(string, BinDataType, Order, OrderByFlags, int)"/>.
	/// </summary>
	public enum OrderByFlags
	{
		/// <summary>
		/// No special comparison behavior.
		/// </summary>
		NONE,

		/// <summary>
		/// Case-insensitive comparison. Only applicable when <see cref="BinDataType.STRING"/> is used.
		/// </summary>
		CASE_INSENSITIVE
	}
}
