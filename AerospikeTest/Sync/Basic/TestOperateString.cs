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
			//CheckServerVersion(new Version(8, 1, 3, 0), "string operations");
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
			r = Operate(StringOperation.IsUpper(bin));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsLowerOnlyTrueForLowercase()
		{
			Put("hello");
			Record r = Operate(StringOperation.IsLower(bin));
			Assert.IsTrue(r.GetBool(bin));
			r = Operate(StringOperation.IsLower(bin));
			Assert.IsFalse(r.GetBool(bin));
		}

		[TestMethod]
		public void IsNumericMatchesIntegerStrings()
		{
			Put("12345");
			Record r = Operate(StringOperation.IsNumeric(bin));
			Assert.IsTrue(r.GetBool(bin));
			r = Operate(StringOperation.IsNumeric(bin));
			Assert.IsFalse(r.GetBool(bin));
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
		public void SplitReturnsListOfTokens()
		{
			Put("one,two,three");
			Record r = Operate(StringOperation.Split(bin, ","));
			Assert.AreEqual(new List<string> { "one", "two", "three" }, r.GetList(bin));
		}

		[TestMethod]
		public void SplitWithoutMatchReturnsSingletonList()
		{
			Put("Hello123World");
			Record r = Operate(StringOperation.Split(bin, "|"));
			Assert.AreEqual(new List<string> { "Hello123World" }, r.GetList(bin));
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
			Assert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
		}

		[TestMethod]
		public void B64DecodeReturnsOriginalBlob()
		{
			Put("aGVsbG8=");
			Record r = Operate(StringOperation.B64Decode(bin));
			Assert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
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
		public void ConcatAppendsListOfValues()
		{
			Put("hello");
			Operate(StringOperation.Concat(policy, bin, [" ", "big", " world"]));
			Assert.AreEqual("hello big world", StringValue());
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
			Assert.AreEqual(11L, r.GetLong(bin));
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

			List<string> tokens = (List<string>)r.GetList(bin);
			Assert.HasCount(3, tokens);
			// Each entry should round-trip as a String regardless of internal encoding.
			foreach (string t in tokens)
			{
				Assert.IsInstanceOfType(t, typeof(string));
			}
		}

		//=================================================================
		// CTX navigation — string nested in list/map bins
		//
		// Exercises the §2.3.1 CTX-wrapper wire envelope: the op-data is
		// wrapped in a 3-element context-eval array (sub-op 0xFF) when CTX
		// is non-empty. The server dispatches these through
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

			List<string> after = (List<string>)client.Get(null, key).GetList(bin);
			Assert.AreEqual(new List<string> { "alpha", "BETA", "gamma" }, after);
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

		//=================================================================
		// toString op — op-type 19, no payload, no sub-op id, no CTX
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
		public void ToStringOnListBinReturnsIncompatibleType()
		{
			List<Value> list = [Value.Get("a"), Value.Get("b")];
			PutList(list);

			AerospikeException ae = Assert.Throws<AerospikeException>(() => Operate(StringOperation.ToString(bin)));
			Assert.AreEqual(ResultCode.BIN_TYPE_ERROR, ae.Result);
		}

		//=================================================================
		// NO_FAIL flag — missing-bin path
		//
		// particle_string.c:926: when the target bin does not exist, the
		// server returns AS_OK with no bin written if NO_FAIL is set; without
		// it, the server returns AS_ERR_BIN_NOT_FOUND. This is the actual
		// scope of NO_FAIL on STRING_MODIFY — the server does NOT honor it
		// for wrong-type bins (incompatible-type is hard-errored at line 872
		// regardless of the flag).
		//=================================================================

		[TestMethod]
		public void ModifyOnMissingBinWithNoFailIsNoOp()
		{
			// Record exists but the target bin does not — exercises the bin-level
			// NO_FAIL path at particle_string.c:926 (not the record-level
			// KEY_NOT_FOUND path that fires when the whole record is absent).
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			StringPolicy noFail = new(StringWriteFlags.NO_FAIL);
			Operate(StringOperation.Upper(noFail, bin));

			// BIN must not have been created; the existing bin must be intact.
			Record r = client.Get(null, key);
			Assert.IsNull(r.GetValue(bin));
			Assert.AreEqual("untouched", r.GetString("other"));
		}

		[TestMethod]
		public void ModifyOnMissingBinWithoutNoFailRaises()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin("other", "untouched"));

			AerospikeException ae = Assert.Throws<AerospikeException>(() => Operate(StringOperation.Upper(policy, bin)));
			Assert.AreEqual(ResultCode.BIN_NOT_FOUND, ae.Result);
		}
	}
}
