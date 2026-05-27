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
	/// Numeric type filter for <see cref="StringOperation.IsNumeric(string, StringNumericType, CTX[])"/>.
	/// </summary>
	public enum StringNumericType
	{
		/// <summary>
		/// Match either an integer or a floating-point number.
		/// </summary>
		ANY = 0,

		/// <summary>
		/// Match only integers.
		/// </summary>
		INT = 1,

		/// <summary>
		/// Match only floating-point numbers.
		/// </summary>
		FLOAT = 2
	}
}
