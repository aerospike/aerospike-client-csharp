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
	/// String operations. Create operations to be passed to the client <see cref="Operate"/>
	/// command for inspecting and modifying string bins.
	/// <para>
	/// Index orientation is left-to-right with codepoint addressing. Negative indexes
	/// count from the end of the string (-1 = last codepoint). Out-of-bounds
	/// indexes are clamped to the valid range; no error is returned.
	/// </para>
	/// String operations require server version 8.1.3 or later. A non-empty <see cref="CTX"/>
	/// argument navigates into a string nested inside a list or map bin; with no CTX
	/// the operation targets the bin itself. The CTX-navigated leaf must already be an
	/// Aerospike string — operations on non-string leaves return
	/// <see cref="AerospikeError.AEROSPIKE_ERR_INCOMPATIBLE_TYPE"/>.
	/// <example>
	/// <code>
	/// // Read: bin "text" = "hello world"
	/// Record r = client.Operate(null, key, StringOperation.strlen("text"));
	/// long len = r.GetLong("text");        // 11
	///
	/// // Modify: uppercase a string nested in a list bin "items" at index 0.
	/// client.Operate(null, key,
	///     StringOperation.upper(StringPolicy.Default, "items", CTX.listIndex(0)));
	/// </code>
	/// </example>
	/// </summary>
	public static class StringOperation {
		// Read ops
		private const int STRLEN = 0;
		private const int SUBSTR = 1;
		private const int CHAR_AT = 2;
		private const int FIND = 3;
		private const int CONTAINS = 4;
		private const int STARTS_WITH = 5;
		private const int ENDS_WITH = 6;
		private const int TO_INTEGER = 7;
		private const int TO_DOUBLE = 8;
		private const int BYTE_LENGTH = 9;
		private const int IS_NUMERIC = 10;
		private const int IS_UPPER = 11;
		private const int IS_LOWER = 12;
		private const int TO_BLOB = 13;
		private const int SPLIT = 14;
		private const int B64_DECODE = 15;
		private const int REGEX_COMPARE = 16;

		// Modify ops
		private const int INSERT = 50;
		private const int OVERWRITE = 51;
		private const int CONCAT = 52;
		private const int SNIP = 53;
		private const int REPLACE = 54;
		private const int REPLACE_ALL = 55;
		private const int UPPER = 56;
		private const int LOWER = 57;
		private const int CASE_FOLD = 58;
		private const int NORMALIZE_NFC = 59;
		private const int TRIM_START = 60;
		private const int TRIM_END = 61;
		private const int TRIM = 62;
		private const int PAD_START = 63;
		private const int PAD_END = 64;
		private const int REPEAT = 65;
		private const int REGEX_REPLACE = 66;

		//-----------------------------------------------------------------
		// Read operations
		//-----------------------------------------------------------------

		/// <summary>
		/// Create string Strlen operation. Returns the number of Unicode codepoints
		/// in the string bin as an int64. This matches <see cref="String.CodePointCount(int, int)"/>
		/// called on the string's full range.
		/// <para>
		/// The returned value is the codepoint count — <strong>not</strong> the count of
		/// user-perceived characters (grapheme clusters). Codepoints and visible characters
		/// agree for ASCII and simple Latin text, but diverge for combining marks, emoji
		/// modifiers, and zero-width-joiner sequences:
		/// <ul>
		/// <li>{@code "é"} encoded as one precomposed codepoint U+00E9 → 1.</li>
		/// <li>{@code "é"} encoded as {@code 'e' + U+0301} (combining acute) → 2, though
		///     it renders as one visible character.</li>
		/// <li>{@code "👍🏽"} (thumbs up + skin-tone modifier) → 2, though it renders as
		///     one emoji.</li>
		/// <li>{@code "👨‍👩‍👧‍👦"} (ZWJ family emoji) → 7, though it renders as one emoji.</li>
		/// </ul>
		/// </para>
		/// <para>
		/// Two related counts that this op does <strong>not</strong> return:
		/// <ul>
		/// <li>String.Length() — counts UTF-16 code units, so a non-BMP codepoint
		///     (e.g. {@code "😀"}) counts as 2 there but 1 here.</li>
		/// <li>UTF-8 byte length — use ByteLength(String, CTX...).</li>
		/// </ul>
		/// </para>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the codepoint count (int64)</returns>
		public static Operation Strlen(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(STRLEN, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Substr operation that reads from <see cref="int"/> start to the end of
		/// the string. Negative indexes count from the end.
		/// <example>
		/// <code>
		/// // "hello world" -> "world"
		/// Record r = client.Operate(null, key, StringOperation.substr("text", 6));
		/// String tail = r.GetString("text");
		/// </code>
		/// </example>
		/// <param name="binName">name of the string bin</param>
		/// <param name="start">starting codepoint index (negative counts from end)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the substring</returns>
		public static Operation Substr(string binName, int start, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SUBSTR, start, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Substr operation that reads <see cref="int"/> length codepoints
		/// starting at <see cref="int"/> start. Negative indexes count from the end of the string.
		/// <example>
		/// <code>
		/// // "hello world" -> "hello"
		/// Record r = client.Operate(null, key, StringOperation.substr("text", 0, 5));
		/// </code>
		/// </example>
		/// <param name="binName">name of the string bin</param>
		/// <param name="start">starting codepoint index (negative counts from end)</param>
		/// <param name="length">number of codepoints to read (clamped to remaining length)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the substring</returns>
		public static Operation Substr(string binName, int start, int length, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SUBSTR, start, length, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string CharAt operation that returns the codepoint at <see cref="int"/> index
		/// as a one-codepoint string. Negative indexes count from the end.
		/// <example>
		/// <code>
		/// // "Hello123World" at index 5 -> "1"
		/// Record r = client.Operate(null, key, StringOperation.charAt("text", 5));
		/// String c = r.GetString("text");
		/// }</code>
		/// </example>
		/// <param name="binName">name of the string bin</param>
		/// <param name="index">codepoint index (negative counts from end)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a single-codepoint string</returns>
		public static Operation CharAt(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(CHAR_AT, index, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string <see cref="Operation"/> that returns the codepoint index of the first
		/// occurrence of <see cref="string"/> needle, or <see cref="int"/> -1 if not found.
		/// <example>
		/// <code>
		/// // "hello world" -> 6
		/// Record r = client.Operate(null, key, StringOperation.find("text", "world"));
		/// long idx = r.GetLong("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="needle">substring to search for</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the codepoint index, or -1 if absent</returns>
		public static Operation Find(string binName, string needle, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(FIND, Value.Get(needle), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Find operation. Returns the codepoint index of the first
		/// occurrence of needle, or -1 if not found.
		/// <example>
		/// <code>
		/// // "hello world" -> 6
		/// Record r = client.Operate(null, key, StringOperation.find("text", "world"));
		/// long idx = r.GetLong("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="needle">substring to search for</param>
		/// <param name="occurrence">1-based occurrence to return (negative counts from the last match)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the codepoint index, or -1 if absent</returns>
		public static Operation Find(string binName, string needle, int occurrence, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(FIND, Value.Get(needle), occurrence, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Contains operation that returns <see cref="bool"/> true if the bin contains
		/// <see cref="string"/> needle as a substring, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "hello world" -> true
		/// Record r = client.Operate(null, key, StringOperation.Contains("text", "hello"));
		/// bool has = r.GetBool("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="needle">substring to test for</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation Contains(string binName, string needle, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(CONTAINS, Value.Get(needle), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		///<summary>
		/// Create string StartsWith operation that returns <see cref="bool"/> true if the bin begins
		/// with <see cref="string"/> prefix, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "Hello123World" -> true
		/// Record r = client.Operate(null, key, StringOperation.StartsWith("text", "Hello"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="prefix">prefix to test for</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation StartsWith(string binName, string prefix, params CTX[] ctx)
{
			byte[] bytes = PackStringOp(STARTS_WITH, Value.Get(prefix), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		///<summary>
		/// Create string EndsWith operation that returns <see cref="bool"/> true if the bin ends
		/// with <see cref="string"/> suffix, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "Hello123World" -> true
		/// Record r = client.Operate(null, key, StringOperation.EndsWith("text", "World"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="suffix">suffix to test for</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation EndsWith(string binName, string suffix, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(ENDS_WITH, Value.Get(suffix), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		///<summary>
		/// Create string ToInteger operation that parses the string as an <see cref="int"/> and returns the parsed value.
		/// Returns <see cref="AerospikeError.AEROSPIKE_ERR_PARAMETER"/> if the bin cannot be parsed as an integer.
		/// <example>
		/// <code>
		/// // "12345" -> 12345
		/// Record r = client.Operate(null, key, StringOperation.ToInteger("text"));
		/// long n = r.GetLong("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the parsed integer value</returns>
		public static Operation ToInteger(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TO_INTEGER, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string ToDouble operation that parses the string as a <see cref="double"/> and returns the parsed value.
		/// Returns <see cref="AerospikeError.AEROSPIKE_ERR_PARAMETER"/> if the bin cannot be parsed as a double.
		/// <example>
		/// <code>
		/// // "3.14" -> 3.14
		/// Record r = client.Operate(null, key, StringOperation.ToDouble("text"));
		/// double v = r.GetDouble("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the parsed double</returns>
		public static Operation ToDouble(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TO_DOUBLE, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string ByteLength operation that returns the number of UTF-8 bytes in
		/// the string (int64). Differs from <see cref="StringOperation.Strlen"/> for non-ASCII content where one
		/// codepoint can encode to multiple bytes.
		/// <example>
		/// <code>
		/// // "hello" -> 5
		/// Record r = client.Operate(null, key, StringOperation.ByteLength("text"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the byte length (int64)</returns>
		public static Operation ByteLength(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(BYTE_LENGTH, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		///<summary>
		/// Create string IsNumeric operation that returns <see cref="bool"/> true if the bin
		/// contains a valid integer or float, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "12345" -> true; "Hello" -> false
		/// Record r = client.Operate(null, key, StringOperation.IsNumeric("text"));
		/// bool numeric = r.GetBool("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation IsNumeric(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(IS_NUMERIC, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string IsNumeric operation that filters by <see cref="StringNumericType"/> and returns <see cref="bool"/> true if the bin
		/// contains a valid integer or float, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "12345" with INT filter -> true
		/// Record r = client.Operate(null, key, StringOperation.IsNumeric("text", StringNumericType.INT));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="numericType">one of the <see cref="StringNumericType"/> constants</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation IsNumeric(string binName, int numericType, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(IS_NUMERIC, numericType, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string IsUpper operation that returns <see cref="bool"/> true if every cased
		/// codepoint in the bin is uppercase, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "HELLO" -> true; "Hello" -> false
		/// Record r = client.Operate(null, key, StringOperation.IsUpper("text"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation IsUpper(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(IS_UPPER, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string IsLower operation that returns <see cref="bool"/> true if every cased
		/// codepoint in the bin is lowercase, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "hello" -> true; "Hello" -> false
		/// Record r = client.Operate(null, key, StringOperation.IsLower("text"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation IsLower(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(IS_LOWER, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string ToBlob operation that returns the UTF-8 bytes of the string
		/// as a blob (byte[]).
		/// <example>
		/// <code>
		/// // "hello" -> [0x68, 0x65, 0x6c, 0x6c, 0x6f]
		/// Record r = client.Operate(null, key, StringOperation.ToBlob("text"));
		/// byte[] bytes = (byte[]) r.getValue("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a byte[] blob</returns>
		public static Operation ToBlob(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TO_BLOB, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Split operation that splits by Unicode codepoint — each
		/// codepoint becomes its own element of the returned list.
		/// <example>
		/// <code>
		/// // "abc" -> ["a", "b", "c"]
		/// Record r = client.Operate(null, key, StringOperation.Split("text"));
		/// List<string> chars = r.GetList<string>("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a list of single-codepoint strings</returns>
		public static Operation Split(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SPLIT, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Split operation that splits the bin by the <see cref="string"/> separator
		/// substring. If the separator is absent the result is a singleton list containing
		/// the whole string.
		/// <example>
		/// <code>
		/// // "one,two,three" with "," -> ["one", "two", "three"]
		/// Record r = client.Operate(null, key, StringOperation.Split("text", ","));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="separator">substring used to split the bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a list of token strings</returns>
		public static Operation Split(string binName, string separator, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SPLIT, Value.Get(separator), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string B64Decode operation that treats the bin as base64-encoded text
		/// and returns the decoded bytes as a blob.
		/// <example>
		/// <code>
		/// // "aGVsbG8=" -> "hello".getBytes()
		/// Record r = client.Operate(null, key, StringOperation.B64Decode("text"));
		/// byte[] decoded = (byte[]) r.GetBytes("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin holding base64 text</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning the decoded byte[]</returns>
		public static Operation B64Decode(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(B64_DECODE, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string RegexCompare operation that matches <see cref="string"/> pattern (ICU regex
		/// syntax) against the bin and returns <see cref="bool"/> true on match, <see cref="bool"/> false otherwise.
		/// <example>
		/// <code>
		/// // "Hello123World" matches "[0-9]+" -> true
		/// Record r = client.Operate(null, key, StringOperation.RegexCompare("text", "[0-9]+"));
		/// bool matched = r.GetBool("text");
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the string bin</param>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation RegexCompare(string binName, string pattern, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(REGEX_COMPARE, Value.Get(pattern), ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string RegexCompare operation that honors <see cref="StringRegexFlags"/>
		/// (e.g. <see cref="StringRegexFlags.CASE_INSENSITIVE"/>). Flag values may be combined
		/// with bitwise OR.
		/// <example>
		/// <code>
		/// // "HELLO" matches "hello" with CASE_INSENSITIVE -> true
		/// Record r = client.Operate(null, key,
		///     StringOperation.RegexCompare("text", "hello", StringRegexFlags.CASE_INSENSITIVE));
		/// </code>
		/// </example>
		/// <param name="binName">name of the string bin</param>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="regexFlags">bitwise-OR of <see cref="StringRegexFlags"/> constants</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>read operation returning a boolean match flag</returns>
		public static Operation RegexCompare(string binName, string pattern, StringRegexFlags regexFlags, params CTX[] ctx) 
		{
			byte[] bytes = PackStringOp(REGEX_COMPARE, Value.Get(pattern), (int)regexFlags, ctx);
			return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		//-----------------------------------------------------------------
		// Modify operations
		//-----------------------------------------------------------------

		/// <summary>
		/// Create string Insert operation that splices <see cref="string"/> value into the bin at
		/// codepoint <see cref="int"/> index. Negative indexes count from the end of the string.
		/// <example>
		/// <code>
		/// // "hello world" + insert " beautiful" at 5 -> "hello beautiful world"
		/// client.Operate(null, key,
		///     StringOperation.insert(StringPolicy.Default, "text", 5, " beautiful"));
		/// }</code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="index">codepoint index at which to insert (negative counts from end)</param>
		/// <param name="value">text to insert</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Insert(StringPolicy policy, string binName, int index, string value, params CTX[] ctx) 
		{
			byte[] bytes = PackStringOp(INSERT, index, Value.Get(value), (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Overwrite operation that overwrites codepoints starting at
		/// codepoint <see cref="int"/> index with <see cref="string"/> value. The result may grow beyond the
		/// original length when <see cref="string"/> value extends past the end.
		/// <example>
		/// <code>
		/// // "hello world" overwrite "earth" at 6 -> "hello earth"
		/// client.Operate(null, key,
		///     StringOperation.overwrite(StringPolicy.Default, "text", 6, "earth"));
		/// }</code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="index">codepoint index at which to start overwriting</param>
		/// <param name="value">text to write</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Overwrite(StringPolicy policy, string binName, int index, string value, params CTX[] ctx) 
		{
			byte[] bytes = PackStringOp(OVERWRITE, index, Value.Get(value), (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Concat operation that appends <see cref="string"/> value to the bin.
		/// <example>
		/// <code>
		/// // "hello" + concat "!" -> "hello!"
		/// client.Operate(null, key,
		///     StringOperation.Concat(StringPolicy.Default, "text", "!"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="value">text to append</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Concat(StringPolicy policy, string binName, string value, params CTX[] ctx) 
		{
			List<Value> list = [Value.Get(value)];
			byte[] bytes = PackStringOp(CONCAT, list, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Concat operation that appends each element of <see cref="List{T}"/> values
		/// to the bin in order.
		/// <example>
		/// <code>
		/// // "hello" + concat [" ", "big", " world"] -> "hello big world"
		/// client.Operate(null, key, StringOperation.Concat(StringPolicy.Default, "text",
		///     Arrays.asList(" ", "big", " world")));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="values">ordered list of strings to append</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Concat(StringPolicy policy, string binName, List<string> values, params CTX[] ctx)
		{
			List<Value> list = ToValueList(values);
			byte[] bytes = PackStringOp(CONCAT, list, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Snip operation that removes codepoints starting at
		/// codepoint <see cref="int"/> start through the end of the string.
		/// <example>
		/// <code>
		/// // "hello world" snip from 5 -> "hello"
		/// client.Operate(null, key,
		///     StringOperation.Snip(StringPolicy.Default, "text", 5));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="start">first codepoint to remove (inclusive)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Snip(StringPolicy policy, string binName, int start, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SNIP, start, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Snip operation that removes the half-open codepoint range
		/// <example>
		/// <code>
		/// // "hello beautiful world" snip [5, 15) -> "hello world"
		/// client.Operate(null, key,
		///     StringOperation.Snip(StringPolicy.Default, "text", 5, 15));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="start">first codepoint to remove (inclusive)</param>
		/// <param name="end">one past the last codepoint to remove (exclusive)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Snip(StringPolicy policy, string binName, int start, int end, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(SNIP, start, end, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Replace operation that replaces the first occurrence of
		/// <see cref="string"/> needle with <see cref="string"/> replacement.
		/// <example>
		/// <code>
		/// // "hello world world" replace "world"->"earth" -> "hello earth world"
		/// client.Operate(null, key,
		///     StringOperation.Replace(StringPolicy.Default, "text", "world", "earth"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="needle">substring to find</param>
		/// <param name="replacement">text to substitute (may be empty to delete the match)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Replace(StringPolicy policy, string binName, string needle, string replacement, params CTX[] ctx)
		{
			List<Value> list = Pair(needle, replacement);
			byte[] bytes = PackStringOp(REPLACE, list, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string ReplaceAll operation that replaces every occurrence of
		/// <see cref="string"/> needle with <see cref="string"/> replacement.
		/// <example>
		/// <code>
		/// // "aabaa" replaceAll "a"->"x" -> "xxbxx"
		/// client.Operate(null, key,
		///     StringOperation.ReplaceAll(StringPolicy.Default, "text", "a", "x"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="needle">substring to find</param>
		/// <param name="replacement">text to substitute (may be empty to delete each match)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation ReplaceAll(StringPolicy policy, string binName, string needle, string replacement, params CTX[] ctx)
		{
			List<Value> list = Pair(needle, replacement);
			byte[] bytes = PackStringOp(REPLACE_ALL, list, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Upper operation that uppercases the bin in place.
		/// <example>
		/// <code>
		/// // "hello world" -> "HELLO WORLD"
		/// client.Operate(null, key, StringOperation.Upper(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Upper(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(UPPER, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Lower operation that lowercases the bin in place.
		/// <example>
		/// <code>
		/// // "HELLO WORLD" -> "hello world"
		/// client.Operate(null, key, StringOperation.Lower(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// </summary>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Lower(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(LOWER, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string CaseFold operation that applies a locale-independent case
		/// fold (lowercase) to the bin. Useful for normalized comparison keys.
		/// <example>
		/// <code>
		/// // "HELLO World" -> "hello world"
		/// client.Operate(null, key, StringOperation.CaseFold(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation CaseFold(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(CASE_FOLD, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string NormalizeNFC operation that normalizes the bin to Unicode
		/// NFC form. Already-normalized strings are unchanged.
		/// <example>
		/// <code>
		/// // "HELLO World" -> "hello world"
		/// client.Operate(null, key, StringOperation.NormalizeNFC(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation NormalizeNFC(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(NORMALIZE_NFC, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string TrimStart operation that removes whitespace from the start
		/// of the bin.
		/// <example>
		/// <code>
		/// // "  hello  " -> "hello  "
		/// client.Operate(null, key, StringOperation.TrimStart(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation TrimStart(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TRIM_START, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string TrimEnd operation that removes whitespace from the end of
		/// the bin.
		/// <example>
		/// <code>
		/// // "  hello  " -> "  hello"
		/// client.Operate(null, key, StringOperation.TrimEnd(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation TrimEnd(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TRIM_END, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Trim operation that removes whitespace from both ends of
		/// the bin.
		/// <example>
		/// <code>
		/// // "  hello world  " -> "hello world"
		/// client.Operate(null, key, StringOperation.Trim(StringPolicy.Default, "text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Trim(StringPolicy policy, string binName, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(TRIM, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string PadStart operation that prepends <see cref="string"/> padString
		/// repeatedly until the bin reaches <see cref="int"/> targetLength codepoints. No-op when the
		/// bin is already at or above the target length.
		/// <example>
		/// <code>
		/// // "hello" pad to 10 with "*" -> "*****hello"
		/// client.Operate(null, key, StringOperation.PadStart(StringPolicy.Default, "text", 10, "*"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="targetLength">codepoint length to pad up to</param>
		/// <param name="padString">text used to fill (repeated as needed)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation PadStart(StringPolicy policy, string binName, int targetLength, string padString, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(PAD_START, targetLength, Value.Get(padString), (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string PadEnd operation that appends <see cref="string"/> padString
		/// repeatedly until the bin reaches <see cref="int"/> targetLength codepoints. No-op when the
		/// bin is already at or above the target length.
		/// <example>
		/// <code>
		/// // "hello" pad to 10 with "." -> "hello....."
		/// client.Operate(null, key, StringOperation.PadEnd(StringPolicy.Default, "text", 10, "."));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="targetLength">codepoint length to pad up to</param>
		/// <param name="padString">text used to fill (repeated as needed)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation PadEnd(StringPolicy policy, string binName, int targetLength, string padString, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(PAD_END, targetLength, Value.Get(padString), (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string Repeat operation that repeats the bin contents <see cref="int"/> count
		/// times.
		/// <example>
		/// <code>
		/// // "ab" repeat 3 -> "ababab"
		/// client.Operate(null, key, StringOperation.Repeat(StringPolicy.Default, "text", 3));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="count">number of repetitions (must be non-negative)</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation Repeat(StringPolicy policy, string binName, int count, params CTX[] ctx)
		{
			byte[] bytes = PackStringOp(REPEAT, count, (int)policy.flags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		/// <summary>
		/// Create string RegexReplace operation that replaces the first match of
		/// <see cref="string"/> pattern with <see cref="string"/> replacement. Pass <see cref="StringRegexFlags.GLOBAL"/>
		/// to replace every match. Flag values from <see cref="StringRegexFlags"/> may be combined
		/// with bitwise OR.
		/// <example>
		/// <code>
		/// // "abc123def456" regexReplace "[0-9]+"->"NUM" with GLOBAL -> "abcNUMdefNUM"
		/// client.Operate(null, key, StringOperation.RegexReplace(StringPolicy.Default, "text", "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="binName">name of the string bin</param>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="replacement">replacement text (must be valid UTF-8)</param>
		/// <param name="regexFlags">bitwise-OR of <see cref="StringRegexFlags"/> constants</param>
		/// <param name="ctx">optional path into a string nested inside a list or map</param>
		/// <returns>modify operation</returns>
		public static Operation RegexReplace
		(
			StringPolicy policy,
			string binName,
			string pattern,
			string replacement,
			StringRegexFlags regexFlags,
			params CTX[] ctx
		)
		{
			List<Value> list = Pair(pattern, replacement);
			// Server's regex_replace op table accepts only [list, regexFlags]; no slot for policy flags.
			byte[] bytes = PackStringOp(REGEX_REPLACE, list, (int)regexFlags, ctx);
			return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
		}

		//-----------------------------------------------------------------
		// Type conversion
		//-----------------------------------------------------------------

		/// <summary>
		/// Create ToString operation that converts an integer, float, string, or
		/// blob bin to its string representation. Returns <see cref="AerospikeError.AEROSPIKE_ERR_INCOMPATIBLE_TYPE"/> for any other bin type.
		/// <para>
		/// Unlike the other builders in this class, <see cref="ToString"/> does not accept a
		/// <see cref="CTX"/>. The other string operations are sent as <see cref="Operation.Type.STRING_READ"/> /
		/// <see cref="Operation.Type.STRING_MODIFY"/> wire ops whose msgpack payload carries the sub-op code,
		/// arguments, and (when <see cref="CTX"/> is non-empty) a <see cref="Operation.Type.TO_STRING"/> wrapper that the server's <see cref="CTX"/>-aware dispatcher unwraps to descend into a list
		/// or map. <see cref="ToString"/> is a separate top-level wire op
		/// (<see cref="Operation.Type.TO_STRING"/>) that carries no payload at all — the bin is
		/// referenced solely by the operation header — and the server-side handler for it
		/// is a different code path that operates on the whole bin particle and never
		/// inspects an op payload, so there is no place to encode a <see cref="CTX"/> wrapper and the
		/// server would not act on it if there were.
		/// </para>
		/// <example>
		/// <code>
		/// // Bin "n" = 42 (integer) -> "42"
		/// Record r = client.operate(null, key, StringOperation.ToString("n"));
		/// String s = r.getString("n");
		/// }</code>
		/// </example>
		/// </summary>
		/// <param name="binName">name of the bin to convert</param>
		/// <returns>read operation returning the string representation of the bin</returns>
		public static Operation ToString(string binName)
		{
			return new Operation(Operation.Type.TO_STRING, binName, Value.AsNull);
		}

		//-----------------------------------------------------------------
		// Private helpers
		//-----------------------------------------------------------------

		private static List<Value> Pair(string a, string b)
		{
			return [Value.Get(a), Value.Get(b)];
		}

		private static List<Value> ToValueList(List<string> strings)
		{
			List<Value> list = new List<Value>(strings.Count);
			foreach (string s in strings) {
				list.Add(Value.Get(s));
			}
			return list;
		}

		//-----------------------------------------------------------------
		// Flat-CTX wire packer (string-op specific).
		//
		// When CTX is empty: emits [SUBOP, args...] — identical to Pack.pack.
		// When CTX is non-empty: emits the FLAT envelope
		//     [0xFF, [ctx_id_1, ctx_value_1, ...], SUBOP, args...]
		// where SUBOP and its args are flattened into the outer array — there
		// is no nested array around them. This matches particle_string.c's
		// string_state_init (line ~735), which reads the sentinel, skips the
		// ctx flat-list with msgpack_sz_vec, then reads the inner op as a
		// direct uint64 (no msgpack_get_list_ele_count_vec call). The CDT
		// module (cdt.c:3671) does call list_ele_count for the inner op and
		// therefore requires a nested layout — the shared Pack.init helper
		// emits that nested form, which is why these string-op overloads exist
		// as a separate path.
		//-----------------------------------------------------------------

		private static void WriteOuterHeader(Packer p, int innerCount, CTX[] ctx)
		{
			bool hasCtx = ctx != null && ctx.Length > 0;
			int outerSize = hasCtx ? (2 + innerCount) : innerCount;
			p.PackArrayBegin(outerSize);
			if (hasCtx)
			{
				p.PackNumber(0xFF);
				p.PackArrayBegin(ctx.Length * 2);
				foreach (CTX c in ctx) {
					p.PackNumber(c.id);
					if (c.value != null)
					{
						c.value.Pack(p);
					}
					else 
					{
						p.PackByteArray(c.exp.Bytes, 0, c.exp.Bytes.Length);
					}
				}
			}
		}

		// [SUBOP]
		private static byte[] PackStringOp(int subop, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 1, ctx);
			p.PackNumber(subop);
			return p.ToByteArray();
		}

		// [SUBOP, v1]
		private static byte[] PackStringOp(int subop, int v1, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 2, ctx);
			p.PackNumber(subop);
			p.PackNumber(v1);
			return p.ToByteArray();
		}

		// [SUBOP, v1, v2]
		private static byte[] PackStringOp(int subop, int v1, int v2, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 3, ctx);
			p.PackNumber(subop);
			p.PackNumber(v1);
			p.PackNumber(v2);
			return p.ToByteArray();
		}

		// [SUBOP, v1, v2, v3]
		private static byte[] PackStringOp(int subop, int v1, int v2, int v3, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 4, ctx);
			p.PackNumber(subop);
			p.PackNumber(v1);
			p.PackNumber(v2);
			p.PackNumber(v3);
			return p.ToByteArray();
		}

		// [SUBOP, v1]  (Value)
		private static byte[] PackStringOp(int subop, Value v1, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 2, ctx);
			p.PackNumber(subop);
			v1.Pack(p);
			return p.ToByteArray();
		}

		// [SUBOP, v1, v2]  (Value, int)
		private static byte[] PackStringOp(int subop, Value v1, int v2, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 3, ctx);
			p.PackNumber(subop);
			v1.Pack(p);
			p.PackNumber(v2);
			return p.ToByteArray();
		}

		// [SUBOP, v1, v2, v3]  (int, Value, int)
		private static byte[] PackStringOp(int subop, int v1, Value v2, int v3, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 4, ctx);
			p.PackNumber(subop);
			p.PackNumber(v1);
			v2.Pack(p);
			p.PackNumber(v3);
			return p.ToByteArray();
		}

		// [SUBOP, list, v2]  (List<Value>, int)
		private static byte[] PackStringOp(int subop, List<Value> list, int v2, CTX[] ctx)
		{
			Packer p = new Packer();
			WriteOuterHeader(p, 3, ctx);
			p.PackNumber(subop);
			p.PackList(list);
			p.PackNumber(v2);
			return p.ToByteArray();
		}
	}
}
