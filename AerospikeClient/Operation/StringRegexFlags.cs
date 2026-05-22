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
	/// Regex flags for <see cref="StringOperation.RegexCompare(string, string, CTX[])/> and
	/// <see cref="StringOperation.RegexReplace"/>. Combine with bitwise OR.
	/// </summary>
	[Flags]
	public enum StringRegexFlags
	{
		/// <summary>
		/// Default. No flags set.
		/// </summary>
		DEFAULT = 0,

		/// <summary>
		/// Case insensitive matching.
		/// </summary>
		CASE_INSENSITIVE = 1 << 0,

		/// <summary>
		/// Treat input as a multi-line string. {@code ^} and {@code $} match
		/// the start and end of any line, not just the start and end of the input.
		/// </summary>
		MULTILINE = 1 << 1,

		/// <summary>
		/// The {@code .} metacharacter matches any character including line terminators.
		/// </summary>
		DOTALL = 1 << 2,

		/// <summary>
		/// Treat only {@code \n} as a line terminator (Unix-style line endings).
		/// </summary>
		UNIX_LINES = 1 << 3,

		/// <summary>
		/// Replace all matches in the input. Only applicable to
		/// <see cref="StringOperation.RegexReplace"/>.
		/// </summary>
		GLOBAL = 1 << 4,
	}
}
