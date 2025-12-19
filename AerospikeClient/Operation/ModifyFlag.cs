/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	/// Flags that control what data is modified by path expression operations.
	/// </summary>
	[Flags]
	public enum ModifyFlag
	{
		/// <summary>
		/// If the expression in the context hits an invalid type, the operation
		/// will fail.  This is the default behavior.
		/// </summary>
		DEFAULT = 0x00,

		/// <summary>
		/// This flag is set when leaf values are to be modified.
		/// </summary>
		APPLY = 0x04,

		/// <summary>
		/// If the expression in the context hits an invalid type (e.g., selects
		/// as an integer when the value is a string), do not fail the operation;
		/// just ignore those elements.  Interpret UNKNOWN as false instead.
		/// </summary>
		NO_FAIL = 0x10,
	}
}
