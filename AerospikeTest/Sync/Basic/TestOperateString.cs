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
using Aerospike.Client;
using System.Collections;

namespace Aerospike.Test
{
	/// <summary>
	/// Integration tests for the string operations exposed by <see cref="StringOperation"/>.
	/// <para>
	/// The tests are organized around the operation behavior they verify rather
	/// than around individual API methods, so each test exercises a single intent
	/// (e.g. "uppercase mutates the bin", "find returns the first match index").
	/// </para>
	/// <para>
	/// String operations require server version 8.1.3+; the tests are skipped
	/// on older clusters via the test assumptions.
	/// </para>
	/// </summary>
	[TestClass]
	public class TestOperateString : TestSync
	{
		private const string bin = "sbin";
		private static readonly Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "stringop-key");
		private static readonly StringPolicy policy = StringPolicy.Default;

		[ClassInitialize]
		public static void ServerVersionCheck(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "string operations");
		}

		//-----------------------------------------------------------------
		// Helpers
		//-----------------------------------------------------------------

		private static void Put(string value)
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, value));
		}

		private static void Put(params Bin[] bins)
		{
			client.Delete(null, key);
			client.Put(null, key, bins);
		}

		private static Record Operate(params Operation[] ops)
		{
			return client.Operate(null, key, ops);
		}

		private static string StringValue()
		{
			return client.Get(null, key).GetString(bin);
		}

		//=================================================================
		// Read operations
		//=================================================================

		[TestMethod]
		public void StrlenReturnsCodepointCount()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(11L, r.GetLong(bin));
		}

		[TestMethod]
		public void StrlenOnEmptyStringIsZero()
		{
			Put("");
			Record r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(0L, r.GetLong(bin));
		}

		[TestMethod]
		public void ByteLengthReturnsUtf8Bytes()
		{
			Put("hello");
			Record r = Operate(StringOperation.ByteLength(bin));
			Assert.AreEqual(5L, r.GetLong(bin));
		}

		//-----------------------------------------------------------------
		// Multi-byte / codepoint-vs-byte tests
		//
		// Server-side indices and strlen are in Unicode code points, not bytes
		// and not Csharp UTF-16 chars. These tests anchor the contract for Csharp
		// callers whose String.length() intuition is UTF-16 code-unit count.
		//-----------------------------------------------------------------

		[TestMethod]
		public void StrlenCountsCodepointsNotCSharpChars()
		{
			// "café" = 4 codepoints; UTF-8 = 5 bytes; Csharp .length() = 4.
			Put("café");
			Record r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(4L, r.GetLong(bin));

			// "日本語" = 3 codepoints; UTF-8 = 9 bytes; Csharp .length() = 3.
			Put("日本語");
			r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(3L, r.GetLong(bin));

			// "👋hi" — emoji is U+1F44B, a supplementary codepoint encoded as
			// a UTF-16 surrogate pair in Csharp. Codepoints = 3; Csharp .length() = 4.
			Put("👋hi");
			r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(3L, r.GetLong(bin));
		}

		[TestMethod]
		public void ByteLengthCountsBytesNotCodepoints()
		{
			Put("café");
			Record r = Operate(StringOperation.ByteLength(bin));
			Assert.AreEqual(5L, r.GetLong(bin));
			Put("日本語");
			r = Operate(StringOperation.ByteLength(bin));
			Assert.AreEqual(9L, r.GetLong(bin));
			Put("👋hi");
			r = Operate(StringOperation.ByteLength(bin));
			Assert.AreEqual(6L, r.GetLong(bin));
		}

		[TestMethod]
		public void SubstrIndexesCodepointsNotBytes()
		{
			// "日本語hi" — substr(start=3, end=5) returns codepoints 3..4 = "hi".
			// A byte-indexed substr would land mid-way through "日" (each CJK char
			// occupies 3 UTF-8 bytes).
			Put("日本語hi");
			Record r = Operate(StringOperation.Substr(bin, 3, 5));
			Assert.AreEqual("hi", r.GetString(bin));
		}

		[TestMethod]
		public void CharAtReturnsWholeCodepoint()
		{
			// charAt at the emoji position should return the full 4-byte codepoint,
			// not a half-surrogate.
			Put("a👋b");
			Record r = Operate(StringOperation.CharAt(bin, 1));
			Assert.AreEqual("👋", r.GetString(bin));
		}

		[TestMethod]
		public void FindReturnsCodepointIndex()
		{
			// "café-world": "world" starts at codepoint 5 (UTF-16 .indexOf would
			// also return 5 here because "é" is a single Csharp char, but the contract
			// is codepoint-indexed).
			Put("café-world");
			Record r = Operate(StringOperation.Find(bin, "world"));
			Assert.AreEqual(5L, r.GetLong(bin));

			// "👋-world": "world" starts at codepoint index 2 (after emoji and dash).
			// Csharp's .IndexOf would return 3 (UTF-16 code-unit index), so this
			// test catches a regression that returned UTF-16 indices.
			Put("👋-world");
			r = Operate(StringOperation.Find(bin, "world"));
			Assert.AreEqual(2L, r.GetLong(bin));
		}

		[TestMethod]
		public void FindAndContainsRequireMatchingNormalizationForm()
		{
			// "café" can be stored as NFC (U+00E9, 1 codepoint, 2 UTF-8 bytes) or NFD
			// (U+0065 U+0301, 2 codepoints, 3 UTF-8 bytes). They render identically but
			// are distinct byte sequences. The server's find / contains uses ICU binary
			// string search — NFC and NFD are NOT considered equal. Callers who need
			// normalization-insensitive search must normalizeNFC the bin (and the needle)
			// first. This test anchors the contract so a future change to ICU comparison
			// mode does not silently flip the behavior.
			string NFC = "caf\u00E9";       // "café" composed
			string NFD = "cafe\u0301";      // "café" decomposed

			Put(NFC);
			// NFC haystack vs NFC needle — match.
			Record r = Operate(StringOperation.Find(bin, NFC));
			Assert.AreEqual(0L, r.GetLong(bin));
			r = Operate(StringOperation.Contains(bin, NFC));
			Assert.IsTrue(r.GetBool(bin));
			// NFC haystack vs NFD needle — no match (byte sequences differ).
			r = Operate(StringOperation.Find(bin, NFD));
			Assert.AreEqual(-1L, r.GetLong(bin));
			r = Operate(StringOperation.Contains(bin, NFD));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void SubstrFromOffsetToEnd()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Substr(bin, 6));
			Assert.AreEqual("world", r.GetString(bin));
		}

		[TestMethod]
		public void SubstrSlicesARange()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Substr(bin, 0, 5));
			Assert.AreEqual("hello", r.GetString(bin));
		}

		[TestMethod]
		public void SubstrSupportsNegativeStart()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Substr(bin, -5));
			Assert.AreEqual("world", r.GetString(bin));
		}

		[TestMethod]
		public void CharAtReturnsSingleCharacter()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.CharAt(bin, 5));
			Assert.AreEqual("1", r.GetString(bin));
		}

		[TestMethod]
		public void FindReturnsIndexOfFirstMatch()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Find(bin, "world"));
			Assert.AreEqual(6L, r.GetLong(bin));
		}

		[TestMethod]
		public void FindReturnsMinusOneWhenAbsent()
		{
			Put("hello world");
			Record r = Operate(StringOperation.Find(bin, "xyz"));
			Assert.AreEqual(-1L, r.GetLong(bin));
		}

		[TestMethod]
		public void FindSkipsOverlappingMatchesAscii()
		{
			// "aa" is a self-overlapping needle (prefix "a" == suffix "a"). After
			// matching at index 0 the search resumes *after* the match (index 2),
			// so the 2nd occurrence is at 2 — not 1. This matches replace() and
			// the ICU usearch path used for non-ASCII haystacks.
			Put("aaaa");
			Record r = Operate(StringOperation.Find(bin, "aa", 1));
			Assert.AreEqual(0L, r.GetLong(bin));
			r = Operate(StringOperation.Find(bin, "aa", 2));
			Assert.AreEqual(2L, r.GetLong(bin));
			r = Operate(StringOperation.Find(bin, "aa", 3));
			Assert.AreEqual(-1L, r.GetLong(bin));
		}

		[TestMethod]
		public void findSkipsOverlappingMatchesUnicode()
		{
			// Same overlap-skip rule on the ICU path. "👋👋" is self-overlapping in
			// codepoints; matches land at codepoint indices 0 and 2, not 0 and 1.
			Put("👋👋👋👋");
			Record r = Operate(StringOperation.Find(bin, "👋👋", 1));
			Assert.AreEqual(0L, r.GetLong(bin));
			r = Operate(StringOperation.Find(bin, "👋👋", 2));
			Assert.AreEqual(2L, r.GetLong(bin));
			r = Operate(StringOperation.Find(bin, "👋👋", 3));
			Assert.AreEqual(-1L, r.GetLong(bin));
		}

		[TestMethod]
		public void ContainsReturnsBoolean()
		{
			Put("hello world");
			Record present = Operate(StringOperation.Contains(bin, "hello"));
			Record absent = Operate(StringOperation.Contains(bin, "xyz"));
			Assert.IsTrue(present.GetBool(bin));
			Assert.IsFalse(absent.GetBool(bin));
		}

		[TestMethod]
		public void StartsWithMatchesPrefix()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.StartsWith(bin, "Hello"));
			Assert.IsTrue(r.GetBool(bin));
			r = Operate(StringOperation.StartsWith(bin, "World"));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void EndsWithMatchesSuffix()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.EndsWith(bin, "World"));
			Assert.IsTrue(r.GetBool(bin));
			r = Operate(StringOperation.EndsWith(bin, "Hello"));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsUpperOnlyTrueForUppercase()
		{
			Put("HELLO");
			Record r = Operate(StringOperation.IsUpper(bin));
			Assert.IsTrue(r.GetBool(bin));
			Put("Hello");
			r = Operate(StringOperation.IsUpper(bin));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsLowerOnlyTrueForLowercase()
		{
			Put("hello");
			Record r = Operate(StringOperation.IsLower(bin));
			Assert.IsTrue(r.GetBool(bin));
			Put("Hello");
			r = Operate(StringOperation.IsLower(bin));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsNumericMatchesIntegerStrings()
		{
			Put("12345");
			Record r = Operate(StringOperation.IsNumeric(bin));
			Assert.IsTrue(r.GetBool(bin));
			Put("hello");
			r = Operate(StringOperation.IsNumeric(bin));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsNumericFloatRequiresFractionalDigit()
		{
			Put("3.14");
			Assert.IsTrue(Operate(StringOperation.IsNumeric(bin, StringNumericType.FLOAT)).GetBool(bin));

			Put("5");
			Assert.IsFalse(Operate(StringOperation.IsNumeric(bin, StringNumericType.FLOAT)).GetBool(bin));
			Assert.IsTrue(Operate(StringOperation.IsNumeric(bin, StringNumericType.ANY)).GetBool(bin));

			Put("5.");
			Assert.IsFalse(Operate(StringOperation.IsNumeric(bin, StringNumericType.FLOAT)).GetBool(bin));

			Put("1e5");
			Assert.IsFalse(Operate(StringOperation.IsNumeric(bin, StringNumericType.FLOAT)).GetBool(bin));
			Assert.IsFalse(Operate(StringOperation.IsNumeric(bin, StringNumericType.ANY)).GetBool(bin));
		}

		[TestMethod]
		public void ToIntegerParsesDigitsAsLong()
		{
			Put("12345");
			Record r = Operate(StringOperation.ToInteger(bin));
			Assert.AreEqual(12345L, r.GetLong(bin));
		}

		[TestMethod]
		public void ToDoubleParsesDecimalNumbers()
		{
			Put("3.14");
			Record r = Operate(StringOperation.ToDouble(bin));
			Assert.AreEqual(3.14, r.GetDouble(bin), 0.001);
		}

		[TestMethod]
		public void ToIntegerRejectsLeadingWhitespace()
		{
			Put(" 123");
			try
			{
				Operate(StringOperation.ToInteger(bin));
				Assert.Inconclusive("Leading-whitespace rejection requires SERVER-1449 on the server.");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, ae.Result);
			}
		}

		[TestMethod]
		public void ToDoubleRejectsLeadingWhitespaceAndHex()
		{
			Put(" 3.14");
			try
			{
				Operate(StringOperation.ToDouble(bin));
				Assert.Inconclusive("Leading-whitespace rejection requires SERVER-1449 on the server.");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, ae.Result);
			}

			Put("0x10");
			try
			{
				Operate(StringOperation.ToDouble(bin));
				Assert.Inconclusive("Hex-literal rejection requires SERVER-1449 on the server.");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, ae.Result);
			}

			Put("5.");
			try
			{
				Operate(StringOperation.ToDouble(bin));
				Assert.Inconclusive("Trailing-decimal rejection requires SERVER-1449 on the server.");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, ae.Result);
			}
		}

		[TestMethod]
		public void ToDoubleAcceptsExponentWhileIsNumericRejects()
		{
			// SERVER-1449: toDouble keeps exponent/inf/nan parsing; isNumeric does not.
			Put("1e5");
			Assert.IsFalse(Operate(StringOperation.IsNumeric(bin)).GetBool(bin));
			Record r = Operate(StringOperation.ToDouble(bin));
			Assert.AreEqual(100000.0, r.GetDouble(bin), 0.001);
		}

		[TestMethod]
		public void SplitReturnsListOfTokens()
		{
			Put("one,two,three");
			Record r = Operate(StringOperation.Split(bin, ","));
			CollectionAssert.AreEqual(new List<object> { "one", "two", "three" }, r.GetList(bin));
		}

		[TestMethod]
		public void SplitWithoutMatchReturnsSingletonList()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.Split(bin, "|"));
			CollectionAssert.AreEqual(new List<object> { "Hello123World" }, r.GetList(bin));
		}

		[TestMethod]
		public void RegexCompareDistinguishesMatchVsMiss()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.RegexCompare(bin, "[0-9]+"));
			Assert.IsTrue(r.GetBool(bin));
			Put("HELLO");
			r = Operate(StringOperation.RegexCompare(bin, "[0-9]+"));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void RegexCompareHonorsCaseInsensitiveFlag()
		{
			Put("HELLO");
			Record r = Operate(StringOperation.RegexCompare(bin, "hello", StringRegexFlags.CASE_INSENSITIVE));
			Assert.IsTrue(r.GetBool(bin));
		}

		[TestMethod]
		public void ToBlobReturnsUtf8Bytes()
		{
			Put("hello");
			Record r = Operate(StringOperation.ToBlob(bin));
			CollectionAssert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
		}

		[TestMethod]
		public void B64DecodeReturnsOriginalBlob()
		{
			Put("aGVsbG8=");
			Record r = Operate(StringOperation.B64Decode(bin));
			CollectionAssert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
		}

		//=================================================================
		// Modify operations
		//=================================================================

		[TestMethod]
		public void UpperMutatesBinInPlace()
		{
			Put("hello world");
			Operate(StringOperation.Upper(policy, bin));
			Assert.AreEqual("HELLO WORLD", StringValue());
		}

		[TestMethod]
		public void LowerMutatesBinInPlace()
		{
			Put("HELLO WORLD");
			Operate(StringOperation.Lower(policy, bin));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void CaseFoldLowercasesIndependentlyOfLocale()
		{
			Put("HELLO World");
			Operate(StringOperation.CaseFold(policy, bin));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void NormalizeNFCLeavesAlreadyNormalizedStringUnchanged()
		{
			Put("hello");
			Operate(StringOperation.NormalizeNFC(policy, bin));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void NormalizeNFCComposesDecomposedSequence()
		{
			// "e\u0301" is the NFD ("decomposed") form of "é": Latin small "e"
			// followed by combining acute accent. normalizeNFC must compose it to
			// U+00E9 (NFC, single codepoint) — proving the op actually transforms
			// non-normalized input, not just the no-op case.
			Put("e\u0301");
			Operate(StringOperation.NormalizeNFC(policy, bin));
			Assert.AreEqual("\u00E9", StringValue());
			// Composed form is 1 codepoint; the decomposed input would be 2.
			Record r = Operate(StringOperation.Strlen(bin));
			Assert.AreEqual(1L, r.GetLong(bin));
		}


		[TestMethod]
		public void InsertAtMiddleSplicesValue()
		{
			Put("hello world");
			Operate(StringOperation.Insert(policy, bin, 5, " beautiful"));
			Assert.AreEqual("hello beautiful world", StringValue());
		}

		[TestMethod]
		public void InsertAtZeroPrependsValue()
		{
			Put("world");
			Operate(StringOperation.Insert(policy, bin, 0, "hello "));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void InsertAtEndAppendsValue()
		{
			Put("hello");
			Operate(StringOperation.Insert(policy, bin, 5, " world"));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void InsertWithNegativeIndexCountsFromEnd()
		{
			Put("hello world");
			Operate(StringOperation.Insert(policy, bin, -5, "big "));
			Assert.AreEqual("hello big world", StringValue());
		}

		[TestMethod]
		public void OverwriteReplacesCharactersStartingAtIndex()
		{
			Put("hello world");
			Operate(StringOperation.Overwrite(policy, bin, 6, "earth"));
			Assert.AreEqual("hello earth", StringValue());
		}

		[TestMethod]
		public void OverwriteAtZeroReplacesPrefix()
		{
			Put("hello world");
			Operate(StringOperation.Overwrite(policy, bin, 0, "HELLO"));
			Assert.AreEqual("HELLO world", StringValue());
		}

		[TestMethod]
		public void OverwriteCanExtendBeyondOriginalLength()
		{
			Put("hello");
			Operate(StringOperation.Overwrite(policy, bin, 3, "ping!"));
			Assert.AreEqual("helping!", StringValue());
		}

		[TestMethod]
		public void OverwriteWithNegativeIndexWrapsFromEnd()
		{
			// SERVER-1409: negative indexes resolve from the end, same as insert/substr.
			Put("hello world");
			try
			{
				Operate(StringOperation.Overwrite(policy, bin, -5, "earth"));
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.PARAMETER_ERROR)
			{
				Assert.Inconclusive("Negative overwrite indexes require SERVER-1409 on the server.");
			}

			Assert.AreEqual("hello earth", StringValue());
		}

		[TestMethod]
		public void OverwriteWithOutOfBoundsIndexRaisesParameter()
		{
			// Overwrite does not clamp; resolved indexes outside [0, len-1] fail.
			Put("hello");
			AssertParamError(StringOperation.Overwrite(policy, bin, 100, "x"));
			AssertParamError(StringOperation.Overwrite(policy, bin, -100, "x"));
		}

		[TestMethod]
		public void SnipRemovesCharacterRange()
		{
			Put("hello beautiful world");
			Operate(StringOperation.Snip(policy, bin, 5, 15));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void SnipFromStartTrimsPrefix()
		{
			Put("hello world");
			Operate(StringOperation.Snip(policy, bin, 0, 6));
			Assert.AreEqual("world", StringValue());
		}

		[TestMethod]
		public void SnipToEndTrimsSuffix()
		{
			Put("hello world");
			Operate(StringOperation.Snip(policy, bin, 5, 11));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void SnipStartOnlyRemovesThroughEnd()
		{
			Put("hello world");
			Operate(StringOperation.Snip(policy, bin, 5));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void ReplaceTouchesOnlyFirstOccurrence()
		{
			Put("hello world world");
			Operate(StringOperation.Replace(policy, bin, "world", "earth"));
			Assert.AreEqual("hello earth world", StringValue());
		}

		[TestMethod]
		public void ReplaceWithNoMatchLeavesBinUnchanged()
		{
			Put("hello world");
			Operate(StringOperation.Replace(policy, bin, "xyz", "abc"));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void ReplaceCanGrowTheString()
		{
			Put("hi world");
			Operate(StringOperation.Replace(policy, bin, "hi", "hello"));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void ReplaceWithEmptyDeletesMatch()
		{
			Put("hello world");
			Operate(StringOperation.Replace(policy, bin, " world", ""));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void ReplaceAllSubstitutesEveryMatch()
		{
			Put("aabaa");
			Operate(StringOperation.ReplaceAll(policy, bin, "a", "x"));
			Assert.AreEqual("xxbxx", StringValue());
		}

		[TestMethod]
		public void ReplaceAllWithNoMatchLeavesBinUnchanged()
		{
			Put("hello");
			Operate(StringOperation.ReplaceAll(policy, bin, "z", "!"));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void ReplaceAllSkipsOverlappingMatches()
		{
			// Self-overlapping needle "aa" in "aaaa": replacement resumes after each
			// match, yielding "XX" — not "XaX" (which would require allowing the
			// 2nd match to start at index 1). Anchors the contract that find() now
			// mirrors.
			Put("aaaa");
			Operate(StringOperation.ReplaceAll(policy, bin, "aa", "X"));
			Assert.AreEqual("XX", StringValue());
		}

		[TestMethod]
		public void TrimRemovesWhitespaceOnBothEnds()
		{
			Put("  hello world  ");
			Operate(StringOperation.Trim(policy, bin));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void TrimOnCleanStringIsNoOp()
		{
			Put("hello");
			Operate(StringOperation.Trim(policy, bin));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void TrimStartRemovesLeadingWhitespaceOnly()
		{
			Put("  hello  ");
			Operate(StringOperation.TrimStart(policy, bin));
			Assert.AreEqual("hello  ", StringValue());
		}

		[TestMethod]
		public void TrimEndRemovesTrailingWhitespaceOnly()
		{
			Put("  hello  ");
			Operate(StringOperation.TrimEnd(policy, bin));
			Assert.AreEqual("  hello", StringValue());
		}

		[TestMethod]
		public void PadStartFillsLeftToTargetLength()
		{
			Put("hello");
			Operate(StringOperation.PadStart(policy, bin, 10, "*"));
			Assert.AreEqual("*****hello", StringValue());
		}

		[TestMethod]
		public void PadStartIsNoOpWhenAlreadyLongEnough()
		{
			Put("hello world");
			Operate(StringOperation.PadStart(policy, bin, 5, "*"));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void PadEndFillsRightToTargetLength()
		{
			Put("hello");
			Operate(StringOperation.PadEnd(policy, bin, 10, "."));
			Assert.AreEqual("hello.....", StringValue());
		}

		[TestMethod]
		public void PadStartRepeatsMultiCharFiller()
		{
			Put("hi");
			Operate(StringOperation.PadStart(policy, bin, 8, "ab"));
			Assert.AreEqual("abababhi", StringValue());
		}

		[TestMethod]
		public void RepeatDuplicatesContents()
		{
			Put("ab");
			Operate(StringOperation.Repeat(policy, bin, 3));
			Assert.AreEqual("ababab", StringValue());
		}

		[TestMethod]
		public void RepeatOnceLeavesBinUnchanged()
		{
			Put("hello");
			Operate(StringOperation.Repeat(policy, bin, 1));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void ConcatAppendsSingleString()
		{
			Put("  hello world  ");
			Operate(StringOperation.Concat(policy, bin, "!"));
			Assert.AreEqual("  hello world  !", StringValue());
		}

		[TestMethod]
		public void SnipThenConcatInOneOperate()
		{
			Put("hello beautiful world");

			Operate(
				StringOperation.Snip(policy, bin, 5, 15),
				StringOperation.Concat(policy, bin, "!"));

			Assert.AreEqual("hello world!", StringValue());
		}

		[TestMethod]
		public void ConcatAppendsListOfValues()
		{
			Put("hello");
			Operate(StringOperation.Concat(policy, bin, [" ", "big", " world"]));
			Assert.AreEqual("hello big world", StringValue());
		}

		[TestMethod]
		public void AppendAddsValueToEnd()
		{
			Put("hello");
			Operate(StringOperation.Append(policy, bin, " world"));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void AppendToEmptyStringYieldsValue()
		{
			Put("");
			Operate(StringOperation.Append(policy, bin, "hi"));
			Assert.AreEqual("hi", StringValue());
		}

		[TestMethod]
		public void AppendPreservesMultibyteCodepoints()
		{
			// Unicode/DBCS-aware: appending a multi-byte string must not corrupt
			// either side. "日本" + "語" -> "日本語" (3 codepoints, 9 UTF-8 bytes).
			Put("日本");
			Operate(StringOperation.Append(policy, bin, "語"));
			Assert.AreEqual("日本語", StringValue());
			Assert.AreEqual(3L, Operate(StringOperation.Strlen(bin)).GetLong(bin));
		}

		[TestMethod]
		public void PrependAddsValueToStart()
		{
			Put("world");
			Operate(StringOperation.Prepend(policy, bin, "hello "));
			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void PrependToEmptyStringYieldsValue()
		{
			Put("");
			Operate(StringOperation.Prepend(policy, bin, "hi"));
			Assert.AreEqual("hi", StringValue());
		}

		[TestMethod]
		public void PrependPreservesMultibyteCodepoints()
		{
			// Unicode/DBCS-aware: prepending a multi-byte string must not corrupt
			// either side. "語" prepended with "日本" -> "日本語".
			Put("語");
			Operate(StringOperation.Prepend(policy, bin, "日本"));
			Assert.AreEqual("日本語", StringValue());
			Assert.AreEqual(3L, Operate(StringOperation.Strlen(bin)).GetLong(bin));
		}

		[TestMethod]
		public void AppendOnMissingBinCreatesTheBinFromEmpty()
		{
			// Create-ops {insert, concat, append, prepend} bootstrap an empty string
			// and create a missing bin. NO_FAIL is irrelevant — the op always succeeds.
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Append(policy, bin, "x"));

			Record r = client.Get(null, key);
			Assert.AreEqual("x", r.GetValue(bin));
			Assert.AreEqual("untouched", r.GetString("other"));
		}

		[TestMethod]
		public void RegexReplaceTargetsFirstMatchByDefault()
		{
			Put("abc123def456");
			Operate(StringOperation.RegexReplace(policy, bin, "[0-9]+", "NUM", StringRegexFlags.DEFAULT));
			Assert.AreEqual("abcNUMdef456", StringValue());
		}

		[TestMethod]
		public void RegexReplaceWithGlobalFlagReplacesEveryMatch()
		{
			Put("abc123def456");
			Operate(StringOperation.RegexReplace(policy, bin, "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
			Assert.AreEqual("abcNUMdefNUM", StringValue());
		}

		[TestMethod]
		public void RegexReplaceWithNoMatchLeavesBinUnchanged()
		{
			Put("hello");
			Operate(StringOperation.RegexReplace(policy, bin, "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void RegexReplacePacksPolicyFlags()
		{
			StringPolicy updateOnly = new(StringWriteFlags.UPDATE_ONLY);
			Operation op = StringOperation.RegexReplace(
				updateOnly, bin, "[0-9]+", "NUM", StringRegexFlags.GLOBAL);
			byte[] bytes = ((Value.BytesValue)op.value).Bytes;
			List<object> args = (List<object>)new Unpacker(bytes, 0, bytes.Length, false).UnpackList();

			Assert.HasCount(4, args);
			Assert.AreEqual((long)StringRegexFlags.GLOBAL, args[2]);
			Assert.AreEqual((long)StringWriteFlags.UPDATE_ONLY, args[3]);
		}

		[TestMethod]
		public void UpdateOnlyAllowsModifyOnExistingBin()
		{
			Put("hello");
			StringPolicy updateOnly = new(StringWriteFlags.UPDATE_ONLY);

			Operate(StringOperation.Append(updateOnly, bin, " world"));

			Assert.AreEqual("hello world", StringValue());
		}

		[TestMethod]
		public void CreateOnlyOnMissingBinCreatesTheBin()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			StringPolicy createOnly = new(StringWriteFlags.CREATE_ONLY);
			Operate(StringOperation.Append(createOnly, bin, "hi"));

			Record r = client.Get(null, key);
			Assert.AreEqual("hi", r.GetString(bin));
			Assert.AreEqual("untouched", r.GetString("other"));
		}

		[TestMethod]
		public void CreateOnlyOnExistingBinRaisesBinExists()
		{
			Put("hello");
			StringPolicy createOnly = new(StringWriteFlags.CREATE_ONLY);

			AerospikeException ae = Assert.Throws<AerospikeException>(() =>
				Operate(StringOperation.Append(createOnly, bin, " world")));
			Assert.AreEqual(ResultCode.BIN_EXISTS_ERROR, ae.Result);
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void CreateOnlyWithNoFailOnExistingBinIsSilentNoOp()
		{
			Put("hello");
			StringPolicy createOnlyNoFail = new(
				StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL);

			Operate(StringOperation.Append(createOnlyNoFail, bin, " world"));

			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void CreateOnlyWithUpdateOnlyRaisesParameterError()
		{
			Put("hello");
			StringPolicy invalid = new(
				StringWriteFlags.CREATE_ONLY | StringWriteFlags.UPDATE_ONLY);

			AerospikeException ae = Assert.Throws<AerospikeException>(() =>
				Operate(StringOperation.Append(invalid, bin, " world")));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void CreateOnlyWithContextRaisesParameterError()
		{
			List<Value> list = [Value.Get("alpha"), Value.Get("beta")];
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, list));

			StringPolicy createOnly = new(StringWriteFlags.CREATE_ONLY);

			AerospikeException ae = Assert.Throws<AerospikeException>(() =>
				Operate(StringOperation.Append(createOnly, bin, "!", CTX.ListIndex(1))));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);

			IList after = client.Get(null, key).GetList(bin);
			CollectionAssert.AreEqual(new List<object> { "alpha", "beta" }, after);
		}

		[TestMethod]
		public void CreateOnlyPacksPolicyFlags()
		{
			StringPolicy createOnly = new(StringWriteFlags.CREATE_ONLY);
			Operation op = StringOperation.Append(createOnly, bin, "x");
			byte[] bytes = ((Value.BytesValue)op.value).Bytes;
			List<object> args = (List<object>)new Unpacker(bytes, 0, bytes.Length, false).UnpackList();

			Assert.AreEqual((long)StringWriteFlags.CREATE_ONLY, args[2]);
		}

		//=================================================================
		// Multi-op pipelines
		//=================================================================

		[TestMethod]
		public void ReadsAcrossMultipleBinsInOneOperate()
		{
			Put(
				new Bin("text", "  hello world  "),
				new Bin("number_str", "12345"),
				new Bin("upper_str", "HELLO"));

			Record r = Operate(
				StringOperation.Strlen("text"),
				StringOperation.ToInteger("number_str"),
				StringOperation.IsUpper("upper_str"));

			// strlen and toInteger return INT; isUpper returns BOOL.
			Assert.AreEqual(15L, r.GetLong("text"));
			Assert.AreEqual(12345L, r.GetLong("number_str"));
			Assert.IsTrue(r.GetBool("upper_str"));
		}

		[TestMethod]
		public void ModifyAndReadInOneOperatePipelineCommitsThenObserves()
		{
			Put("  hello world  ");

			Record r = Operate(
				StringOperation.Trim(policy, bin),
				StringOperation.Upper(policy, bin),
				StringOperation.Strlen(bin));

			// strlen runs after trim+upper so it sees the post-modification length.
			// String ops set RESPOND_ALL_OPS (like BIT/EXP/HLL/MAP), so the three ops
			// targeting the same bin come back as an ordered per-op result list rather
			// than a single collapsed value. strlen runs last and therefore observes the
			// post-trim+upper length.
			IList results = r.GetList(bin);
			Assert.AreEqual(11L, results[results.Count - 1]);
			Assert.AreEqual("HELLO WORLD", StringValue());
		}

		[TestMethod]
		public void ChainedReplaceAllAndPaddingComposeAsExpected()
		{
			Put("aabaa");

			Operate(
				StringOperation.ReplaceAll(policy, bin, "a", "x"),
				StringOperation.PadEnd(policy, bin, 10, "."));

			Assert.AreEqual("xxbxx.....", StringValue());
		}

		[TestMethod]
		public void SplitResultListEntriesAreReadableStrings()
		{
			Put("one,two,three");
			Record r = Operate(StringOperation.Split(bin, ","));

			IList tokens = r.GetList(bin);
			Assert.HasCount(3, tokens);
			// Each entry should round-trip as a String regardless of internal encoding.
			foreach (object t in tokens)
			{
				Assert.IsInstanceOfType(t, typeof(string));
			}
		}

		//=================================================================
		// CTX navigation — string nested in list/map bins
		//
		// Exercises the §2.3.1 CTX-wrapper wire envelope: the op-data is
		// wrapped in a 3-element CONTEXT_EVAL array (0xFF sentinel) whose
		// third element is a nested [inner_op, ...args] list when CTX is
		// non-empty (SERVER-1483). The server dispatches these through
		// as_bin_string_modify_ctx_tr / its read-side twin, which is a
		// separate code path from the top-level-bin variant exercised above.
		//=================================================================

		private static void PutList(List<Value> values)
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, values));
		}

		private static void PutMap(Dictionary<Value, Value> entries)
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, entries));
		}

		[TestMethod]
		public void ReadOpOnStringNestedInList()
		{
			// list = ["alpha", "beta", "hello world"]; strlen at index 2 = 11
			List<Value> list = [Value.Get("alpha"), Value.Get("beta"), Value.Get("hello world")];
			PutList(list);

			Record r = Operate(StringOperation.Strlen(bin, CTX.ListIndex(2)));
			Assert.AreEqual(11L, r.GetLong(bin));
		}

		[TestMethod]
		public void CtxOnNonStringLeafRaisesIncompatibleType()
		{
			// list = ["alpha", 42]; strlen at index 1 targets a non-string leaf.
			List<Value> list = [Value.Get("alpha"), Value.Get(42)];
			PutList(list);

			AerospikeException ae = Assert.Throws<AerospikeException>(() =>
				Operate(StringOperation.Strlen(bin, CTX.ListIndex(1))));
			Assert.IsTrue(
				ae.Result == ResultCode.OP_NOT_APPLICABLE || ae.Result == ResultCode.BIN_TYPE_ERROR,
				$"Unexpected result code: {ae.Result}");
		}

		[TestMethod]
		public void ReadBooleanOpOnStringNestedInMap()
		{
			// map = {"a": "Hello", "b": "World"}; startsWith("World","Wor") = true
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("a")] = Value.Get("Hello"),
				[Value.Get("b")] = Value.Get("World")
			};
			PutMap(map);

			Record r = Operate(StringOperation.StartsWith(bin, "Wor", CTX.MapKey(Value.Get("b"))));
			Assert.IsTrue(r.GetBool(bin));
		}

		[TestMethod]
		public void ModifyOpOnStringNestedInList()
		{
			// list = ["alpha", "beta", "gamma"]; upper at index 1 -> "BETA"
			List<Value> list = [Value.Get("alpha"), Value.Get("beta"), Value.Get("gamma")];
			PutList(list);

			Operate(StringOperation.Upper(policy, bin, CTX.ListIndex(1)));

			IList after = client.Get(null, key).GetList(bin);
			CollectionAssert.AreEqual(new List<object> { "alpha", "BETA", "gamma" }, after);
		}

		[TestMethod]
		public void ModifyOpOnStringNestedInMap()
		{
			// map = {"a": "hello world", "b": "foo"}; replace at key "a"
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("a")] = Value.Get("hello world"),
				[Value.Get("b")] = Value.Get("foo")
			};
			PutMap(map);

			Operate(StringOperation.Replace(policy, bin, "world", "earth",
				CTX.MapKey(Value.Get("a"))));

			var after = client.Get(null, key).GetMap(bin);
			Assert.AreEqual("hello earth", after["a"]);
			Assert.AreEqual("foo", after["b"]);
		}

		[TestMethod]
		public void ModifyOpOnStringDeeplyNestedListInMap()
		{
			// map = {"items": ["one", "two", "three"]}; upper at items[1] -> "TWO"
			List<Value> inner = [Value.Get("one"), Value.Get("two"), Value.Get("three")];
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("items")] = Value.Get(inner)
			};
			PutMap(map);

			Operate(StringOperation.Upper(policy, bin,
				CTX.MapKey(Value.Get("items")), CTX.ListIndex(1)));

			var after = client.Get(null, key).GetMap(bin);
			List<object> items = (List<object>)after["items"];
			CollectionAssert.AreEqual(new List<object> { "one", "TWO", "three" }, items);
		}

		[TestMethod]
		public void AppendOnStringNestedInList()
		{
			// list = ["alpha", "beta", "gamma"]; append "!" at index 1 -> "beta!"
			List<Value> list = [Value.Get("alpha"), Value.Get("beta"), Value.Get("gamma")];
			PutList(list);

			Operate(StringOperation.Append(policy, bin, "!", CTX.ListIndex(1)));

			IList after = client.Get(null, key).GetList(bin);
			CollectionAssert.AreEqual(new List<object> { "alpha", "beta!", "gamma" }, after);
		}

		[TestMethod]
		public void ModifyOpWithFlagsOnStringNestedInList()
		{
			// append takes 1-2 args, so its trailing flags slot is optional. Under CTX
			// the flags sit in the nested inner array, whose own header declares the
			// arity — in the flat envelope they were indistinguishable from a 2nd arg.
			List<Value> list = [Value.Get("alpha"), Value.Get("beta"), Value.Get("gamma")];
			PutList(list);

			StringPolicy noFail = new(StringWriteFlags.NO_FAIL);
			Operate(StringOperation.Append(noFail, bin, "!", CTX.ListIndex(1)));

			IList after = client.Get(null, key).GetList(bin);
			CollectionAssert.AreEqual(new List<object> { "alpha", "beta!", "gamma" }, after);
		}

		[TestMethod]
		public void PrependOnStringNestedInMap()
		{
			// map = {"a": "world", "b": "foo"}; prepend "hello " at key "a"
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("a")] = Value.Get("world"),
				[Value.Get("b")] = Value.Get("foo")
			};
			PutMap(map);

			Operate(StringOperation.Prepend(policy, bin, "hello ",
				CTX.MapKey(Value.Get("a"))));

			var after = client.Get(null, key).GetMap(bin);
			Assert.AreEqual("hello world", after["a"]);
			Assert.AreEqual("foo", after["b"]);
		}

		[TestMethod]
		public void TrimOnStringNestedInListCarriesPolicyFlags()
		{
			// list = ["alpha", "  beta  ", "gamma"]; trim at index 1 -> "beta"
			// Exercises CTX + modify op with optional trailing policy flags.
			List<Value> list = [Value.Get("alpha"), Value.Get("  beta  "), Value.Get("gamma")];
			PutList(list);

			Operate(StringOperation.Trim(policy, bin, CTX.ListIndex(1)));

			IList after = client.Get(null, key).GetList(bin);
			CollectionAssert.AreEqual(new List<object> { "alpha", "beta", "gamma" }, after);
		}

		//=================================================================
		// ToString op — op-type 19, no payload, no sub-op id, no CTX
		//
		// Spec §2.6 and §4.1: covers integer/float/string/blob -> string
		// conversions, plus the INCOMPATIBLE_TYPE error path for list/map
		// bins that the wire format cannot represent.
		//=================================================================

		[TestMethod]
		public void ToStringConvertsIntegerBinToString()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, 42));
			Record r = Operate(StringOperation.ToString(bin));
			Assert.AreEqual("42", r.GetString(bin));
		}

		[TestMethod]
		public void ToStringConvertsDoubleBinToString()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, 3.14));
			Record r = Operate(StringOperation.ToString(bin));
			// Float-to-string formatting is server-side; assert it parses back.
			Assert.AreEqual(3.14, Double.Parse(r.GetString(bin)), 0.0001);
		}

		[TestMethod]
		public void ToStringOnStringBinIsIdentity()
		{
			Put("hello");
			Record r = Operate(StringOperation.ToString(bin));
			Assert.AreEqual("hello", r.GetString(bin));
		}

		[TestMethod]
		public void ToStringConvertsBlobBinToString()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, "hi"u8.ToArray()));
			Record r = Operate(StringOperation.ToString(bin));
			// Server's blob-to-string representation is well-defined for ASCII bytes.
			Assert.AreEqual("hi", r.GetString(bin));
		}

		[TestMethod]
		public void ToStringOnBlobWithInvalidUtf8RaisesOpNotApplicable()
		{
			// {0xED, 0xA0, 0x80} is the UTF-8 encoding of U+D800 (ill-formed
			// surrogate). The server's blob→string conversion validates the bytes
			// via cf_str_is_valid_utf8 and rejects non-well-formed input with
			// OP_NOT_APPLICABLE (mirrors the server's ToStringTest.Blob_InvalidUtf8
			// unit test). Companion to TestStringInvalidUtf8 which exercises the
			// same fixture on the read/modify ops via a STRING-typed bin.
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin,
				new byte[] { (byte)0xED, (byte)0xA0, (byte)0x80 }));
			AerospikeException ae = Assert.Throws<AerospikeException>(() => Operate(StringOperation.ToString(bin)));
			Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, ae.Result);
		}

		[TestMethod]
		public void ToStringOnListBinReturnsIncompatibleType()
		{
			List<Value> list = [Value.Get("a"), Value.Get("b")];
			PutList(list);

			AerospikeException ae = Assert.Throws<AerospikeException>(() => Operate(StringOperation.ToString(bin)));
			Assert.AreEqual(ResultCode.BIN_TYPE_ERROR, ae.Result);
		}

		//=================================================================
		// Missing-bin path
		//
		// Behavior keys off the op, not the flag. The eight additive
		// create-ops {insert, overwrite, concat, append, prepend, padStart,
		// padEnd, repeat} create a missing bin from an empty string;
		// transform/subtractive ops are a silent no-op (success, bin not
		// created). There is no BIN_NOT_FOUND path. NO_FAIL no longer governs
		// this path — it only suppresses an in-op execution failure (and still
		// does not suppress BIN_TYPE_ERROR on a wrong-type bin).
		//=================================================================

		[TestMethod]
		public void ModifyOnMissingBinIsNoOp()
		{
			// A non-create modify op (upper) on a missing bin is a silent no-op
			// (success, bin not created) regardless of NO_FAIL — there is no
			// BIN_NOT_FOUND path. Record exists but the target bin does not.
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Upper(policy, bin));

			// BIN must not have been created; the existing bin must be intact.
			Record r = client.Get(null, key);
			Assert.AreEqual(null, r.GetValue(bin));
			Assert.AreEqual("untouched", r.GetString("other"));
		}

		[TestMethod]
		public void NoFailDoesNotChangeMissingBinNoOp()
		{
			// The missing-bin no-op for non-create ops is flag-independent; NO_FAIL
			// neither creates the bin nor raises an error.
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			StringPolicy noFail = new StringPolicy(StringWriteFlags.NO_FAIL);
			Operate(StringOperation.Upper(noFail, bin));

			Record r = client.Get(null, key);
			Assert.AreEqual(null, r.GetValue(bin));
			Assert.AreEqual("untouched", r.GetString("other"));
		}

		// All eight additive ops create a missing bin from empty in server 8.1.3
		// (string ops + SERVER-97 PR 1452, which adds overwrite/repeat/padStart/
		// padEnd to the create-op set). Transform/subtractive ops still no-op.
		// append is covered above in the append section.

		[TestMethod]
		public void InsertOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Insert(policy, bin, 0, "hi"));

			Assert.AreEqual("hi", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void ConcatOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Concat(policy, bin, "hi"));

			Assert.AreEqual("hi", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void PrependOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Prepend(policy, bin, "hi"));

			Assert.AreEqual("hi", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void OverwriteOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Overwrite(policy, bin, 0, "hi"));

			Assert.AreEqual("hi", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void PadStartOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.PadStart(policy, bin, 5, "x"));

			Assert.AreEqual("xxxxx", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void PadEndOnMissingBinCreatesTheBinFromEmpty()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.PadEnd(policy, bin, 5, "x"));

			Assert.AreEqual("xxxxx", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void RepeatOnMissingBinCreatesAnEmptyBin()
		{
			// repeat(n) on empty = "" — the bin is created holding an empty string
			// (server test: expect_string_bin(b, "")).
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			Operate(StringOperation.Repeat(policy, bin, 3));

			Assert.AreEqual("", client.Get(null, key).GetString(bin));
		}

		//=================================================================
		// Prepare / parameter errors
		//
		// These exercise the server's prepare-phase validation
		// (particle_string.c: find occurrence != 0, empty/negative pad
		// arguments, repeat count >= 0, regex_replace pattern compile).
		// Without NO_FAIL, invalid regex patterns surface as PARAMETER_ERROR.
		// With NO_FAIL, regex_replace returns the unmodified source string.
		//=================================================================

		private static void AssertParamError(Operation op)
		{
			AerospikeException ae = Assert.Throws<AerospikeException>(() => Operate(op));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
		}

		[TestMethod]
		public void FindWithZeroOccurrenceRaisesParameter()
		{
			Put("hello");
			// 0 is reserved as "no occurrence"; the server's find prepare rejects it.
			AssertParamError(StringOperation.Find(bin, "x", 0));
		}

		[TestMethod]
		public void PadStartWithEmptyPadStringRaisesParameter()
		{
			Put("hello");
			AssertParamError(StringOperation.PadStart(policy, bin, 10, ""));
		}

		[TestMethod]
		public void PadEndWithEmptyPadStringRaisesParameter()
		{
			Put("hello");
			AssertParamError(StringOperation.PadEnd(policy, bin, 10, ""));
		}

		[TestMethod]
		public void PadStartWithNegativeTargetRaisesParameter()
		{
			Put("hello");
			AssertParamError(StringOperation.PadStart(policy, bin, -1, "*"));
		}

		[TestMethod]
		public void RepeatWithNegativeCountRaisesParameter()
		{
			Put("hello");
			AssertParamError(StringOperation.Repeat(policy, bin, -1));
		}

		[TestMethod]
		public void RegexReplaceNoFailSuppressesInvalidPattern()
		{
			// regexReplace carries both a regex-flags and a policy-flags argument; the
			// policy slot is the third and last. NO_FAIL there suppresses the compile
			// failure the test above asserts, leaving the bin untouched.
			Put("hello");

			StringPolicy noFail = new(StringWriteFlags.NO_FAIL);
			Operate(StringOperation.RegexReplace(
				noFail, bin, "[unclosed", "NUM", StringRegexFlags.DEFAULT));
			Assert.AreEqual("hello", StringValue());
		}

		[TestMethod]
		public void RegexReplaceWithInvalidPatternRaisesParameterError()
		{
			Put("hello");
			// Unclosed character class — PCRE2 compile fails inside the op.
			// Server returns PARAMETER_ERROR (the server doc table lists this row as
			// "OP_NOT_APPLICABLE / error"; observed behavior on 8.1.3 is PARAMETER).
			AssertParamError(StringOperation.RegexReplace(
				policy, bin, "[unclosed", "NUM", StringRegexFlags.DEFAULT));
		}
	}
}
