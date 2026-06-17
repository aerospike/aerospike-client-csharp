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
	/// String expression generator. Produces <see cref="Exp"/> nodes that read or transform
	/// string values inside an Aerospike <see cref="Expression"/>. Mirrors the operations
	/// exposed by <see cref="StringOperation"/>, but composes
	/// inside expressions instead of being sent as standalone operate ops.
	/// <para>
	/// Each builder takes an <see cref="Exp"/> src that produces the string to operate on.
	/// Common sources:
	/// <ul>
	/// <li><see cref="Exp.StringBin(string)"/> — read a string bin.</li>
	/// <li><see cref="Exp.Val(string)"/> — a string literal.</li>
	/// <li>Another <see cref="StringExp"/> expression — chains read/transform ops.</li>
	/// </ul>
	/// </para>
	/// <para>
	/// Modify-style expressions (e.g. <see cref="StringExp.Upper"/>, <see cref="StringExp.Replace"/>) return the
	/// <strong>modified string value</strong>; they do not mutate the underlying bin.
	/// To persist a change, write the returned value back via
	/// <see cref="Exp.Build"/> or use
	/// <see cref="StringOperation"/> for direct ops.
	/// </para>
	/// <para>
	/// Index orientation is left-to-right with codepoint addressing. Negative indexes
	/// count from the end of the string (<code>Exp.Val(-1)</code> = last codepoint). Out-of-bounds
	/// indexes are clamped to the valid range; no error is returned.
	/// </para>
	/// <para>
	/// Unlike <see cref="StringOperation"/>, these builders
	/// do <strong>not</strong> accept a <see cref="CTX"/>. To apply
	/// a string expression to a value nested inside a list or map, compose with
	/// <see cref="ListExp.GetByIndex"/> or
	/// <see cref="MapExp.GetByKey"/> (which do take CTX) to extract
	/// the leaf, then pass the resulting <see cref="Exp"/> as <see cref="Exp"/> src.
	/// </para>
	/// <para>
	/// String expressions require server version 8.1.3 or later.
	/// </para>
	/// </summary>
	/// <example>
	/// <code>
	/// // Filter records whose "name" bin starts with "hello".
	/// Expression filter = Exp.Build(
	///     StringExp.StartsWith(Exp.Val("hello"), Exp.StringBin("name")));
	/// </code>
	/// </example>
	public sealed class StringExp
	{
		private const int MODULE = 3; // CALL_STRING
		private const int MODULE_REPR = 4; // CALL_REPR

		//-----------------------------------------------------------------
		// Read expressions
		//-----------------------------------------------------------------

		/// <summary>
		/// Create expression that returns the number of Unicode codepoints in <see cref="Exp"/> src
		/// as an int64.
		/// <para>
		/// The returned value is the codepoint count — <strong>not</strong> the count of
		/// user-perceived characters (grapheme clusters). They agree for ASCII / simple
		/// Latin text but diverge for combining marks, emoji modifiers, and ZWJ sequences
		/// (see <see cref="StringOperation.Strlen"/> for examples).
		/// </para>
		/// <para>
		/// For UTF-8 byte length, use <see cref="StringExp.ByteLength(Exp)"/>.
		/// </para>
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" -> 11
		/// Exp len = StringExp.Strlen(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>integer-typed expression yielding the codepoint count</returns>
		public static Exp Strlen(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.STRLEN);
			return AddRead(src, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns the substring of <paramref name="src"/> from codepoint
		/// <see cref="Exp"/> start to the end. Negative <see cref="Exp"/> start counts from the end of the
		/// string.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" from 6 -> "world"
		/// Exp tail = StringExp.Substr(Exp.Val(6), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="start">starting codepoint index (negative counts from end)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the substring</returns>
		public static Exp Substr(Exp start, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.SUBSTR, start);
			return AddRead(src, bytes, Exp.Type.STRING);
		}

		/// <summary>
		/// Create expression that returns the codepoints of <see cref="Exp"/> src from <see cref="Exp"/> start
		/// (inclusive) to <see cref="Exp"/> end (exclusive). Negative indexes count from the end.
		/// If, after negative-index normalization, <see cref="Exp"/> start >= <see cref="Exp"/> end, the result is the
		/// empty string.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" [0, 5) -> "hello"
		/// Exp head = StringExp.Substr(Exp.Val(0), Exp.Val(5), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="start">starting codepoint index, inclusive (negative counts from end)</param>
		/// <param name="end">end codepoint index, exclusive (negative counts from end)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the substring</returns>
		public static Exp Substr(Exp start, Exp end, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.SUBSTR, start, end);
			return AddRead(src, bytes, Exp.Type.STRING);
		}

		/// <summary>
		/// Create expression that returns the codepoint at <see cref="Exp"/> index of <see cref="Exp"/> src
		/// as a one-codepoint string. Negative indexes count from the end.
		/// </summary>
		/// <example>
		/// <code>
		/// // "Hello123World" at 5 -> "1"
		/// Exp c = StringExp.CharAt(Exp.Val(5), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="index">codepoint index (negative counts from end)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding a single-codepoint string</returns>
		public static Exp CharAt(Exp index, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.CHAR_AT, index);
			return AddRead(src, bytes, Exp.Type.STRING);
		}

		/// <summary>
		/// Create expression that returns the codepoint index of the first occurrence of
		/// <see cref="Exp"/> needle in <see cref="Exp"/> src, or <code>Exp.Val(-1)</code> if not found.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" find "world" -> 6
		/// Exp idx = StringExp.Find(Exp.Val("world"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="needle">substring to search for (any expression yielding a string)</param>
		/// <param name="src">source string expression</param>
		/// <returns>integer-typed expression: codepoint index, or -1 if absent</returns>
		public static Exp Find(Exp needle, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.FIND, needle);
			return AddRead(src, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns the codepoint index of the <see cref="Exp"/> occurrence-th
		/// match of <see cref="Exp"/> needle (<code>Exp.Val(1)</code> = first, <code>Exp.Val(-1)</code> = last), or <code>Exp.Val(-1)</code>
		/// if not found.
		/// </summary>
		/// <example>
		/// <code>
		/// // "ababab" 2nd occurrence of "ab" -> 2
		/// Exp idx = StringExp.Find(Exp.Val("ab"), Exp.Val(2), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="needle">substring to search for (any expression yielding a string)</param>
		/// <param name="occurrence">1-based occurrence to return (negative counts from the last)</param>
		/// <param name="src">source string expression</param>
		/// <returns>integer-typed expression: codepoint index, or -1 if absent</returns>
		public static Exp Find(Exp needle, Exp occurrence, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.FIND, needle, occurrence);
			return AddRead(src, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> src contains <see cref="Exp"/> needle as a
		/// substring. Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Expression filter = Exp.Build(
		///     StringExp.Contains(Exp.Val("hello"), Exp.StringBin("text")));
		/// </code>
		/// </example>
		/// <param name="needle">substring to test for</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the substring matched</returns>
		public static Exp Contains(Exp needle, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.CONTAINS, needle);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> src begins with <see cref="Exp"/> prefix.
		/// Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp matched = StringExp.StartsWith(Exp.Val("Hello"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="prefix">prefix to test for</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the prefix matched</returns>
		public static Exp StartsWith(Exp prefix, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.STARTS_WITH, prefix);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> src ends with <see cref="Exp"/> suffix.
		/// Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp matched = StringExp.EndsWith(Exp.Val("World"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="suffix">suffix to test for</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the suffix matched</returns>
		public static Exp EndsWith(Exp suffix, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.ENDS_WITH, suffix);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that parses <see cref="Exp"/> src as an int64. The expression returns
		/// an error <code>Exp.Val(0)</code> if the source cannot be parsed as an integer.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp n = StringExp.ToInteger(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>integer-typed expression yielding the parsed int64</returns>
		public static Exp ToInteger(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TO_INTEGER);
			return AddRead(src, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that parses <see cref="Exp"/> src as a 64-bit float. The expression
		/// returns an error <code>Exp.Val(0)</code> if the source cannot be parsed as a double.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp v = StringExp.ToDouble(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>float-typed expression yielding the parsed double</returns>
		public static Exp ToDouble(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TO_DOUBLE);
			return AddRead(src, bytes, Exp.Type.FLOAT);
		}

		/// <summary>
		/// Create expression that returns the UTF-8 byte length of <see cref="Exp"/> src as an int64.
		/// Differs from <see cref="StringExp.Strlen(Exp)"/> for non-ASCII content where one codepoint can encode
		/// to multiple bytes.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp len = StringExp.ByteLength(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>integer-typed expression yielding the UTF-8 byte length</returns>
		public static Exp ByteLength(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.BYTE_LENGTH);
			return AddRead(src, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> src contains a valid integer or
		/// float literal. Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp numeric = StringExp.IsNumeric(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the source is numeric</returns>
		public static Exp IsNumeric(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.IS_NUMERIC);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> src parses as a number of the
		/// requested <see cref="StringNumericType"/>. Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// // restrict to integer-only validation
		/// Exp isInt = StringExp.IsNumeric(StringNumericType.INT, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="numericType">one of the <see cref="StringNumericType"/> constants</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the source is numeric of the given type</returns>
		public static Exp IsNumeric(StringNumericType numericType, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.IS_NUMERIC, (int)numericType);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that tests whether every cased codepoint in <see cref="Exp"/> src is
		/// uppercase. Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp upper = StringExp.IsUpper(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the source is uppercase</returns>
		public static Exp IsUpper(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.IS_UPPER);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that tests whether every cased codepoint in <see cref="Exp"/> src is
		/// lowercase. Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>	
		/// Exp lower = StringExp.IsLower(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the source is lowercase</returns>
		public static Exp IsLower(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.IS_LOWER);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>
		/// Create expression that returns the UTF-8 bytes of <see cref="Exp"/> src as a blob.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp blob = StringExp.ToBlob(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>blob-typed expression yielding the UTF-8 byte array</returns>
		public static Exp ToBlob(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TO_BLOB);
			return AddRead(src, bytes, Exp.Type.BLOB);
		}

		/// <summary>
		/// Create expression that splits <see cref="Exp"/> src by Unicode codepoint — each codepoint
		/// becomes its own list element.
		/// </summary>
		/// <example>
		/// <code>
		/// // "abc" -> ["a", "b", "c"]
		/// Exp parts = StringExp.Split(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression</param>
		/// <returns>list-typed expression yielding a list of single-codepoint strings</returns>
		public static Exp Split(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.SPLIT);
			return AddRead(src, bytes, Exp.Type.LIST);
		}

		/// <summary>
		/// Create expression that splits <see cref="Exp"/> src by the <see cref="Exp"/> separator substring.
		/// If the separator is absent, the result is a singleton list containing the whole source.
		/// </summary>
		/// <example>
		/// <code>
		/// // "one,two,three" with "," -> ["one", "two", "three"]
		/// Exp tokens = StringExp.Split(Exp.Val(","), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="separator">substring used to split the source</param>
		/// <param name="src">source string expression</param>
		/// <returns>list-typed expression yielding the token list</returns>
		public static Exp Split(Exp separator, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.SPLIT, separator);
			return AddRead(src, bytes, Exp.Type.LIST);
		}

		/// <summary>
		/// Create expression that base64-decodes <see cref="Exp"/> src and returns the decoded bytes as a blob.
		/// </summary>
		/// <example>
		/// <code>
		/// // "aGVsbG8=" -> System.Text.Encoding.UTF8.GetBytes("hello")
		/// Exp decoded = StringExp.B64Decode(Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="src">source string expression holding base64 text</param>
		/// <returns>blob-typed expression yielding the decoded bytes</returns>
		public static Exp B64Decode(Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.B64_DECODE);
			return AddRead(src, bytes, Exp.Type.BLOB);
		}

		/// <summary>
		/// Create expression that tests whether <see cref="Exp"/> pattern (ICU regex syntax) matches <see cref="Exp"/> src.
		/// Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// // matches if "text" contains any digit run
		/// Exp matched = StringExp.RegexCompare(Exp.Val("[0-9]+"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the pattern matched</returns>
		public static Exp RegexCompare(Exp pattern, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.REGEX_COMPARE, pattern);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		/// <summary>	
		/// Create expression that tests whether <see cref="Exp"/> pattern matches <see cref="Exp"/> src under
		/// the supplied <see cref="StringRegexFlags"/>. Flags can be combined with bitwise OR.
		/// Returns <c>true</c> on match, <c>false</c> otherwise.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp matched = StringExp.RegexCompare(
		///     Exp.Val("hello"), StringRegexFlags.CASE_INSENSITIVE,
		///     Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="regexFlags">bitwise-OR of <see cref="StringRegexFlags"/> constants</param>
		/// <param name="src">source string expression</param>
		/// <returns>boolean-typed expression indicating whether the pattern matched</returns>
		public static Exp RegexCompare(Exp pattern, StringRegexFlags regexFlags, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.REGEX_COMPARE, pattern, (int)regexFlags);
			return AddRead(src, bytes, Exp.Type.BOOL);
		}

		//-----------------------------------------------------------------
		// Modify expressions
		//-----------------------------------------------------------------

		/// <summary>
		///	Create expression that splices <see cref="Exp"/> value into <see cref="Exp"/> src at codepoint
		/// <see cref="Exp"/> index and returns the resulting string. Negative indexes count from the
		/// end. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" insert " beautiful" at 5 -> "hello beautiful world"
		/// Exp out = StringExp.Insert(StringPolicy.Default,
		///     Exp.Val(5), Exp.Val(" beautiful"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="index">codepoint index at which to insert (negative counts from end)</param>
		/// <param name="value">text to insert</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Insert(StringPolicy policy, Exp index, Exp value, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.INSERT, index, value, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that overwrites codepoints in <see cref="Exp"/> src starting at codepoint
		/// <see cref="Exp"/> index with <see cref="Exp"/> value, returning the resulting string. The result may
		/// grow beyond the original length when <see cref="Exp"/> value extends past the end. Does not
		/// modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world" overwrite "earth" at 6 -> "hello earth"
		/// Exp out = StringExp.Overwrite(StringPolicy.Default,
		///     Exp.Val(6), Exp.Val("earth"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="index">codepoint index at which to start overwriting</param>
		/// <param name="value">text to write</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Overwrite(StringPolicy policy, Exp index, Exp value, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.OVERWRITE, index, value, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that concatenates <see cref="Exp"/> values (a list of strings) onto
		/// <see cref="Exp"/> src in order, returning the resulting string. Does not modify the
		/// underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello" + [" ", "big", " world"] -> "hello big world"
		/// Exp out = StringExp.Concat(StringPolicy.Default,
		///     Exp.ListVal(new List&lt;string&gt; { " ", "big", " world" }),
		///     Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="values">expression yielding a list of strings to append</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Concat(StringPolicy policy, Exp values, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.CONCAT, values, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that appends <see cref="Exp"/> value to the end of <see cref="Exp"/> src, returning the resulting string. Does not modify the underlying bin.
		/// </summary>
		/// <para>
		/// Unicode/DBCS-aware counterpart to the legacy byte-level append; provides a consistent
		/// string-package interface alongside <see cref="Exp"/>.
		/// </para>
		/// <example>
		/// <code>
		/// // "hello" + append "!" -> "hello!"
		/// Exp out = StringExp.Append(StringPolicy.Default, Exp.Val("!"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="value">expression yielding the string to append to the end</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Append(StringPolicy policy, Exp value, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.APPEND, value, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that prepends <see cref="Exp"/> value to the start of <see cref="Exp"/> src, returning the resulting string. Does not modify the underlying bin.
		/// </summary>
		/// <para>
		/// Unicode/DBCS-aware counterpart to the legacy byte-level prepend; provides a consistent
		/// string-package interface alongside <see cref="Exp"/>.
		/// </para>
		/// <example>
		/// <code>
		/// // "world" prepend "hello " -> "hello world"
		/// Exp out = StringExp.Prepend(StringPolicy.Default, Exp.Val("hello "), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="value">expression yielding the string to prepend to the start</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Prepend(StringPolicy policy, Exp value, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.PREPEND, value, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that removes the half-open codepoint range <see cref="Exp"/> [start, end)
		/// from <see cref="Exp"/> src and returns the resulting string. Does not modify the underlying
		/// bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello beautiful world" snip [5, 15) -> "hello world"
		/// Exp out = StringExp.Snip(StringPolicy.Default,
		///     Exp.Val(5), Exp.Val(15), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="start">first codepoint to remove (inclusive)</param>
		/// <param name="end">one past the last codepoint to remove (exclusive)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Snip(StringPolicy policy, Exp start, Exp end, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.SNIP, start, end, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that replaces the first occurrence of <see cref="Exp"/> needle in
		/// <see cref="Exp"/> src with <see cref="Exp"/> replacement and returns the resulting string. Does not
		/// modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello world world" replace "world"->"earth" -> "hello earth world"
		/// Exp out = StringExp.Replace(StringPolicy.Default,
		///     Exp.Val("world"), Exp.Val("earth"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="needle">substring to find</param>
		/// <param name="replacement">text to substitute (may be empty to delete the match)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp Replace(StringPolicy policy, Exp needle, Exp replacement, Exp src)
		{
			byte[] bytes = PackReplace(StringOperation.REPLACE, needle, replacement, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that replaces every occurrence of <see cref="Exp"/> needle in
		/// <see cref="Exp"/> src with <see cref="Exp"/> replacement and returns the resulting string. Does not
		/// modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "aabaa" replaceAll "a"->"x" -> "xxbxx"
		/// Exp out = StringExp.ReplaceAll(StringPolicy.Default,
		///     Exp.Val("a"), Exp.Val("x"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="needle">substring to find</param>
		/// <param name="replacement">text to substitute (may be empty to delete each match)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp ReplaceAll(StringPolicy policy, Exp needle, Exp replacement, Exp src)
		{
			byte[] bytes = PackReplace(StringOperation.REPLACE_ALL, needle, replacement, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src uppercased. Does not modify the
		/// underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp out = StringExp.Upper(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the uppercased string</returns>
		public static Exp Upper(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.UPPER, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src lowercased. Does not modify the
		/// underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp out = StringExp.Lower(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the lowercased string</returns>
		public static Exp Lower(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.LOWER, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src case-folded (locale-independent
		/// lowercase). Useful for normalized comparison keys. Does not modify the underlying
		/// bin.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp out = StringExp.CaseFold(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the case-folded string</returns>
		public static Exp CaseFold(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.CASE_FOLD, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src normalized to Unicode NFC form.
		/// Already-normalized strings are unchanged. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// Exp out = StringExp.NormalizeNFC(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the NFC-normalized string</returns>
		public static Exp NormalizeNFC(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.NORMALIZE_NFC, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src with whitespace removed from the start.
		/// Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "  hello  " -> "hello  "
		/// Exp out = StringExp.TrimStart(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the left-trimmed string</returns>
		public static Exp TrimStart(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TRIM_START, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src with whitespace removed from the end.
		/// Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "  hello  " -> "  hello"
		/// Exp out = StringExp.TrimEnd(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the right-trimmed string</returns>
		public static Exp TrimEnd(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TRIM_END, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src with whitespace removed from both
		/// ends. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "  hello world  " -> "hello world"
		/// Exp out = StringExp.Trim(StringPolicy.Default, Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the trimmed string</returns>
		public static Exp Trim(StringPolicy policy, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.TRIM, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that prepends <see cref="Exp"/> padString to <see cref="Exp"/> src repeatedly until
		/// the result reaches <see cref="Exp"/> targetLength codepoints. No-op when the source is
		/// already at or above the target length. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello" pad to 10 with "*" -> "*****hello"
		/// Exp out = StringExp.PadStart(StringPolicy.Default,
		///     Exp.Val(10), Exp.Val("*"), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="targetLength">codepoint length to pad up to</param>
		/// <param name="padString">text used to fill (repeated as needed)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the padded string</returns>
		public static Exp PadStart(StringPolicy policy, Exp targetLength, Exp padString, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.PAD_START, targetLength, padString, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that appends <see cref="Exp"/> padString to <see cref="Exp"/> src repeatedly until
		/// the result reaches <see cref="Exp"/> targetLength codepoints. No-op when the source is
		/// already at or above the target length. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "hello" pad to 10 with "." -> "hello....."
		/// Exp out = StringExp.PadEnd(StringPolicy.Default,
		///     Exp.Val(10), Exp.Val("."), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="targetLength">codepoint length to pad up to</param>
		/// <param name="padString">text used to fill (repeated as needed)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the padded string</returns>
		public static Exp PadEnd(StringPolicy policy, Exp targetLength, Exp padString, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.PAD_END, targetLength, padString, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that returns <see cref="Exp"/> src repeated <see cref="Exp"/> count times. Does
		/// not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "ab" repeat 3 -> "ababab"
		/// Exp out = StringExp.Repeat(StringPolicy.Default,
		///     Exp.Val(3), Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">write policy controlling NO_FAIL semantics</param>
		/// <param name="count">number of repetitions (must be non-negative)</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the repeated string</returns>
		public static Exp Repeat(StringPolicy policy, Exp count, Exp src)
		{
			byte[] bytes = PackUtil.Pack(StringOperation.REPEAT, count, (int)policy.flags);
			return AddModify(src, bytes);
		}

		/// <summary>
		/// Create expression that replaces matches of <see cref="Exp"/> pattern (ICU regex syntax) in
		/// <see cref="Exp"/> src with <see cref="Exp"/> replacement and returns the resulting string. Pass
		/// <see cref="StringRegexFlags.GLOBAL"/> to replace every match. Flag values may be
		/// combined with bitwise OR. Does not modify the underlying bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // "abc123def456" regexReplace "[0-9]+"->"NUM" with GLOBAL -> "abcNUMdefNUM"
		/// Exp out = StringExp.RegexReplace(StringPolicy.Default,
		///     Exp.Val("[0-9]+"), Exp.Val("NUM"), StringRegexFlags.GLOBAL,
		///     Exp.StringBin("text"));
		/// </code>
		/// </example>
		/// <param name="policy">	kept for API symmetry with the other modify ops; unused — the
		///						regex_replace server op does not accept policy flags
		///						(see implementation note)</param>
		/// <param name="pattern">ICU-syntax regex pattern (must be valid UTF-8)</param>
		/// <param name="replacement">replacement text (must be valid UTF-8)</param>
		/// <param name="regexFlags">bitwise-OR of <see cref="StringRegexFlags"/> constants</param>
		/// <param name="src">source string expression</param>
		/// <returns>string-typed expression yielding the modified string</returns>
		public static Exp RegexReplace(StringPolicy policy, Exp pattern, Exp replacement, StringRegexFlags regexFlags, Exp src)
		{
			byte[] bytes = PackRegexReplace(pattern, replacement, (int)regexFlags);
			return AddModify(src, bytes);
		}


		//-----------------------------------------------------------------
		// Type conversion expression
		//-----------------------------------------------------------------

		/// <summary>
		/// Create expression that returns the string representation of <see cref="Exp"/> src, where
		/// <see cref="Exp"/> src may be any expression yielding an integer, float, string, or blob
		/// value. Returns an error for any other source type.
		/// </summary>
		/// <example>
		/// <code>
		/// // integer bin "n" = 42 -> "42"
		/// Exp s = StringExp.ToString(Exp.IntBin("n"));
		/// </code>
		/// </example>
		/// <param name="src">source expression (integer, float, string, or blob)</param>
		/// <returns>string-typed expression yielding the string representation</returns>
		public static Exp ToString(Exp src)
		{
			byte[] bytes = ReprPayload();
			return new Exp.Module(src, bytes, (int)Exp.Type.STRING, MODULE_REPR);
		}

		//-----------------------------------------------------------------
		// Private helpers
		//-----------------------------------------------------------------

		private static Exp.Module AddRead(Exp src, byte[] bytes, Exp.Type retType)
		{
			return new Exp.Module(src, bytes, (int)retType, MODULE);
		}

		private static Exp.Module AddModify(Exp src, byte[] bytes)
		{
			return new Exp.Module(src, bytes, (int)Exp.Type.STRING, MODULE | Exp.MODIFY);
		}

		// QUOTED opcode (mirrors Exp.QUOTED = 126).
		// Used to mark an inner msgpack list as a literal — without it, the server's
		// expression compiler at exp.c:3289 treats any bare nested list inside a CALL
		// payload as a sub-expression and recursively compiles its first element as an
		// opcode, which fails with PARAMETER_ERROR for our string-pair lists.
		private const int QUOTED = 126;

		// [cmd, [needle, repl], flags] — needle/replacement nested inside a 2-element list.
		// Specialized packing method. Leaving in StringExp instead of moving to Pack since the 
		// structure is specific to string replace operations and doesn't fit the usual pattern
		// of a command followed by a flat list of arguments.
		private static byte[] PackReplace(int command, Exp needle, Exp replacement, int flags)
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			packer.PackArrayBegin(2);
			packer.PackNumber(QUOTED);
			packer.PackArrayBegin(2);
			needle.Pack(packer);
			replacement.Pack(packer);
			packer.PackNumber(flags);
			return packer.ToByteArray();
		}

		// [REGEX_REPLACE, [pattern, repl], regexFlags] — 3 elements.
		// Server's regex_replace op table accepts only [list, regexFlags]; no slot for
		// policy flags (max_args=2 in particle_string.c:476). Specialized packing method
		// kept in StringExp instead of moving to Pack since the structure is specific to
		// string replace operations and doesn't fit the usual pattern of a command
		// followed by a flat list of arguments.
		private static byte[] PackRegexReplace(Exp pattern, Exp replacement, int regexFlags)
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(3);
			packer.PackNumber(StringOperation.REGEX_REPLACE);
			packer.PackArrayBegin(2);
			packer.PackNumber(QUOTED);
			packer.PackArrayBegin(2);
			pattern.Pack(packer);
			replacement.Pack(packer);
			packer.PackNumber(regexFlags);
			return packer.ToByteArray();
		}

		// Single-zero payload [0] for CALL_REPR (StringExp.toString). The server's
		// parse_op_call at exp.c:3244 rejects an empty list (ele_count == 0), so the
		// payload must contain at least one element. The CALL_REPR dispatcher at
		// exp.c:5019 ignores the sub-op id and goes straight to as_bin_to_string, so
		// the value carried here is unused. The spec previously documented this as `[]`;
		// the server is the source of truth — see §2.7 in the cross-client spec.
		private static byte[] ReprPayload()
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(1);
			packer.PackNumber(0);
			return packer.ToByteArray();
		}
	}
}