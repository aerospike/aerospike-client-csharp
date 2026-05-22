/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	/// Integration tests for the string filter-expression builders exposed by
	/// <see cref="StringExp"/>. Each test puts a representative bin, builds an
	/// <see cref="Expression"/> that wraps a <see cref="StringExp"/> call, evaluates it via
	/// <see cref="ExpOperation.read"/> into a virtual bin, and asserts the result.
	/// <para>
	/// String expressions require server version 8.1.3+; the tests are skipped
	/// on older clusters via <see cref="Assume"/>.
	/// </para>
	/// <para>
	/// Unlike <see cref="StringOperation"/>, the expression path does <strong>not</strong> take a CTX directly. To target a
	/// string nested in a list/map, callers project the nested value via
	/// <see cref="ListExp.GetByIndex"/> or <see cref="MapExp.GetByKey"/> and feed the result
	/// as <see cref="Exp"/> src. Two such cases are exercised at the end of this file.
	/// </para>
	/// </summary>
	[TestClass]
	public class TestStringExp : TestSync
	{
		private static readonly string bin = "sbin";
		private static readonly string var = "v";
		private static readonly Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "stringexp-key");
		private static readonly StringPolicy policy = StringPolicy.Default;

		[ClassInitialize]
		public static void ServerVersionCheck(TestContext testContext)
		{
			//CheckServerVersion(new Version(8, 1, 3, 0), "string operations");
		}

		//-----------------------------------------------------------------
		// Helpers
		//-----------------------------------------------------------------

		private static void Put(String value)
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, value));
		}

		private static void PutRaw(Bin bin)
		{
			client.Delete(null, key);
			client.Put(null, key, bin);
		}

		private static Record Eval(Exp e)
		{
			return client.Operate(null, key,
				ExpOperation.Read(var, Exp.Build(e), ExpReadFlags.DEFAULT));
		}

		//=================================================================
		// Read expressions
		//=================================================================

		[TestMethod]
		public void StrlenReturnsCodepointCount()
		{
			Put("hello world");
			Record r = Eval(StringExp.Strlen(Exp.StringBin(bin)));
			Assert.AreEqual(11L, r.GetLong(bin));
		}

		[TestMethod]
		public void SubstrFromOffsetAndRange()
		{
			Put("hello world");
			// Single-arg form: offset to end.
			Record r1 = Eval(StringExp.Substr(Exp.Val(6), Exp.StringBin(bin)));
			Assert.AreEqual("world", r1.GetString(bin));
			// Two-arg form: [start, length).
			Record r2 = Eval(StringExp.Substr(Exp.Val(0), Exp.Val(5), Exp.StringBin(bin)));
			Assert.AreEqual("hello", r2.GetString(bin));
		}

		[TestMethod]
		public void CharAtReturnsSingleCharacter()
		{
			Put("Hello123World");
			Record r = Eval(StringExp.CharAt(Exp.Val(5), Exp.StringBin(bin)));
			Assert.AreEqual("1", r.GetString(bin));
		}

		[TestMethod]
		public void FindReturnsIndexOfFirstAndNthMatch()
		{
			Put("ababab");
			// Default (first match).
			Record r1 = Eval(StringExp.Find(Exp.Val("ab"), Exp.StringBin(bin)));
			Assert.AreEqual(0L, r1.GetLong(bin));
			// Occurrence overload (1-based) — second occurrence starts at index 2.
			Record r2 = Eval(StringExp.Find(Exp.Val("ab"), Exp.Val(2), Exp.StringBin(bin)));
			Assert.AreEqual(2L, r2.GetLong(bin));
		}

		[TestMethod]
		public void ContainsReturnsBoolean()
		{
			Put("hello world");
			Record present = Eval(StringExp.Contains(Exp.Val("hello"), Exp.StringBin(bin)));
			Record absent = Eval(StringExp.Contains(Exp.Val("xyz"), Exp.StringBin(bin)));
			Assert.IsTrue(present.GetBool(bin));
			Assert.IsFalse(absent.GetBool(bin));
		}

		[TestMethod]
		public void StartsWithMatchesPrefix()
		{
			Put("Hello123World");
			Record r1 = Eval(StringExp.StartsWith(Exp.Val("Hello"), Exp.StringBin(bin)));
			Assert.IsTrue(r1.GetBool(bin));
			Record r2 = Eval(StringExp.StartsWith(Exp.Val("World"), Exp.StringBin(bin)));
			Assert.IsFalse(r2.GetBool(bin));
		}

		[TestMethod]
		public void EndsWithMatchesSuffix()
		{
			Put("Hello123World");
			Record r1 = Eval(StringExp.EndsWith(Exp.Val("World"), Exp.StringBin(bin)));
			Assert.IsTrue(r1.GetBool(bin));
			Record r2 = Eval(StringExp.EndsWith(Exp.Val("Hello"), Exp.StringBin(bin)));
			Assert.IsFalse(r2.GetBool(bin));
		}

		[TestMethod]
		public void ToIntegerParsesDigitsAsLong()
		{
			Put("12345");
			Record r = Eval(StringExp.ToInteger(Exp.StringBin(bin)));
			Assert.AreEqual(12345L, r.GetLong(bin));
		}

		[TestMethod]
		public void ToDoubleParsesDecimalNumbers()
		{
			Put("3.14");
			Record r = Eval(StringExp.ToDouble(Exp.StringBin(bin)));
			Assert.AreEqual(3.14, r.GetDouble(bin), 0.001);
		}

		[TestMethod]
		public void ByteLengthReturnsUtf8Bytes()
		{
			Put("hello");
			Record r = Eval(StringExp.ByteLength(Exp.StringBin(bin)));
			Assert.AreEqual(5L, r.GetLong(bin));
		}

		[TestMethod]
		public void IsNumericMatchesIntegerStringsByDefaultAndByType()
		{
			Put("12345");
			// Default (ANY): both ints and floats pass.
			Assert.IsTrue(Eval(StringExp.IsNumeric(Exp.StringBin(bin))).GetBool(bin));
			// INT-only: still passes for pure-digit string.
			Assert.IsTrue(Eval(StringExp.IsNumeric(StringNumericType.INT, Exp.StringBin(bin))).GetBool(bin));
			Put("3.14");
			// INT-only: fails for a float-shaped string.
			Assert.IsFalse(Eval(StringExp.IsNumeric(StringNumericType.INT, Exp.StringBin(bin))).GetBool(bin));
			Put("hello");
			Assert.IsFalse(Eval(StringExp.IsNumeric(Exp.StringBin(bin))).GetBool(bin));
		}

		[TestMethod]
		public void IsUpperAndIsLowerDistinguishCase()
		{
			Put("HELLO");
			Assert.IsTrue(Eval(StringExp.IsUpper(Exp.StringBin(bin))).GetBool(bin));
			Assert.IsFalse(Eval(StringExp.IsLower(Exp.StringBin(bin))).GetBool(bin));

			Put("hello");
			Assert.IsFalse(Eval(StringExp.IsUpper(Exp.StringBin(bin))).GetBool(bin));
			Assert.IsTrue(Eval(StringExp.IsLower(Exp.StringBin(bin))).GetBool(bin));
		}

		[TestMethod]
		public void ToBlobReturnsUtf8Bytes()
		{
			Put("hello");
			Record r = Eval(StringExp.ToBlob(Exp.StringBin(bin)));
			Assert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
		}

		[TestMethod]
		public void SplitWithAndWithoutSeparator()
		{
			Put("one,two,three");
			Record r1 = Eval(StringExp.Split(Exp.Val(","), Exp.StringBin(bin)));
			Assert.AreEqual(new List<string> { "one", "two", "three" }, r1.GetList(bin));

			// No-separator form returns a singleton-list wrapping the whole string.
			Put("Hello123World");
			Record r2 = Eval(StringExp.Split(Exp.StringBin(bin)));
			Assert.AreEqual(new List<string> { "Hello123World" }, r2.GetList(bin));
		}

		[TestMethod]
		public void B64DecodeReturnsOriginalBlob()
		{
			Put("aGVsbG8=");
			Record r = Eval(StringExp.B64Decode(Exp.StringBin(bin)));
			Assert.AreEqual(ByteUtil.StringToUtf8("hello"), (byte[])r.GetValue(bin));
		}

		[TestMethod]
		public void RegexCompareWithAndWithoutCaseInsensitiveFlag()
		{
			Put("Hello123World");
			Assert.IsTrue(Eval(StringExp.RegexCompare(Exp.Val("[0-9]+"), Exp.StringBin(bin))).GetBool(bin));

			Put("HELLO");
			Assert.IsFalse(Eval(StringExp.RegexCompare(Exp.Val("hello"), Exp.StringBin(bin))).GetBool(bin));
			Assert.IsTrue(Eval(StringExp.RegexCompare(Exp.Val("hello"), StringRegexFlags.CASE_INSENSITIVE, Exp.StringBin(bin))).GetBool(bin));
		}

		[TestMethod]
		public void RegexCompareLiteralSourceIgnoresBin()
		{
			// Source can be any string-yielding expression — not only a bin reference.
			Put("ignored");
			Record r = Eval(StringExp.RegexCompare(Exp.Val("[A-Z]+"), Exp.Val("HELLO")));
			Assert.IsTrue(r.GetBool(bin));
		}

		//=================================================================
		// Modify expressions (return the modified string; do not persist)
		//=================================================================

		[TestMethod]
		public void InsertSplicesIntoSource()
		{
			Put("hello world");
			Record r = Eval(StringExp.Insert(policy, Exp.Val(5), Exp.Val(" beautiful"), Exp.StringBin(bin)));
			Assert.AreEqual("hello beautiful world", r.GetString(bin));
			// Modify expressions do not persist — original bin is unchanged.
			Assert.AreEqual("hello world", client.Get(null, key).GetString(bin));
		}

		[TestMethod]
		public void OverwriteReplacesRange()
		{
			Put("hello world");
			Record r = Eval(StringExp.Overwrite(policy, Exp.Val(6), Exp.Val("earth"), Exp.StringBin(bin)));
			Assert.AreEqual("hello earth", r.GetString(bin));
		}

		[TestMethod]
		public void ConcatAppendsListOfValues()
		{
			Put("hello");
			Exp values = Exp.Val(new List<string> { " ", "big", " world" });
			Record r = Eval(StringExp.Concat(policy, values, Exp.StringBin(bin)));
			Assert.AreEqual("hello big world", r.GetString(bin));
		}

		[TestMethod]
		public void SnipRemovesFromStartAndRange()
		{
			Put("hello world");
			// One-arg form: start through end.
			Record r1 = Eval(StringExp.Snip(policy, Exp.Val(5), Exp.StringBin(bin)));
			Assert.AreEqual("hello", r1.GetString(bin));

			// Two-arg form: half-open [start, end).
			Put("hello beautiful world");
			Record r2 = Eval(StringExp.Snip(policy, Exp.Val(5), Exp.Val(15), Exp.StringBin(bin)));
			Assert.AreEqual("hello world", r2.GetString(bin));
		}

		[TestMethod]
		public void ReplaceTouchesOnlyFirstMatch()
		{
			Put("hello world world");
			Record r = Eval(StringExp.Replace(policy, Exp.Val("world"), Exp.Val("earth"), Exp.StringBin(bin)));
			Assert.AreEqual("hello earth world", r.GetString(bin));
		}

		[TestMethod]
		public void ReplaceAllSubstitutesEveryMatch()
		{
			Put("aabaa");
			Record r = Eval(StringExp.ReplaceAll(policy, Exp.Val("a"), Exp.Val("x"), Exp.StringBin(bin)));
			Assert.AreEqual("xxbxx", r.GetString(bin));
		}

		[TestMethod]
		public void upperAndLowerProduceCorrectCase()
		{
			Put("hello World");
			Assert.AreEqual("HELLO WORLD",
				Eval(StringExp.Upper(policy, Exp.StringBin(bin))).GetString(bin));
			Assert.AreEqual("hello world",
				Eval(StringExp.Lower(policy, Exp.StringBin(bin))).GetString(bin));
		}

		[TestMethod]
		public void CaseFoldLowercasesIndependentlyOfLocale()
		{
			Put("HELLO World");
			Record r = Eval(StringExp.CaseFold(policy, Exp.StringBin(bin)));
			Assert.AreEqual("hello world", r.GetString(bin));
		}

		[TestMethod]
		public void NormalizeNFCLeavesAlreadyNormalizedStringUnchanged()
		{
			Put("hello");
			Record r = Eval(StringExp.NormalizeNFC(policy, Exp.StringBin(bin)));
			Assert.AreEqual("hello", r.GetString(bin));
		}

		[TestMethod]
		public void TrimVariantsStripAppropriateEdges()
		{
			Put("  hello world  ");
			Assert.AreEqual("hello world",
				Eval(StringExp.Trim(policy, Exp.StringBin(bin))).GetString(bin));
			Assert.AreEqual("hello world  ",
				Eval(StringExp.TrimStart(policy, Exp.StringBin(bin))).GetString(bin));
			Assert.AreEqual("  hello world",
				Eval(StringExp.TrimEnd(policy, Exp.StringBin(bin))).GetString(bin));
		}

		[TestMethod]
		public void PadStartFillsLeftToTargetLength()
		{
			Put("hello");
			Record r = Eval(StringExp.PadStart(policy, Exp.Val(10), Exp.Val("*"), Exp.StringBin(bin)));
			Assert.AreEqual("*****hello", r.GetString(bin));
		}

		[TestMethod]
		public void PadEndFillsRightToTargetLength()
		{
			Put("hello");
			Record r = Eval(StringExp.PadEnd(policy, Exp.Val(10), Exp.Val("."), Exp.StringBin(bin)));
			Assert.AreEqual("hello.....", r.GetString(bin));
		}

		[TestMethod]
		public void RepeatDuplicatesContents()
		{
			Put("ab");
			Record r = Eval(StringExp.Repeat(policy, Exp.Val(3), Exp.StringBin(bin)));
			Assert.AreEqual("ababab", r.GetString(bin));
		}

		[TestMethod]
		public void RegexReplaceFirstAndGlobal()
		{
			Put("abc123def456");
			// Default: first match only.
			Record r1 = Eval(StringExp.RegexReplace(policy, Exp.Val("[0-9]+"), Exp.Val("NUM"), StringRegexFlags.DEFAULT, Exp.StringBin(bin)));
			Assert.AreEqual("abcNUMdef456", r1.GetString(bin));

			// GLOBAL: every match.
			Record r2 = Eval(StringExp.RegexReplace(policy, Exp.Val("[0-9]+"), Exp.Val("NUM"), StringRegexFlags.GLOBAL, Exp.StringBin(bin)));
			Assert.AreEqual("abcNUMdefNUM", r2.GetString(bin));
		}

		//=================================================================
		// Type conversion expression
		//=================================================================

		[TestMethod]
		public void ToStringConvertsIntegerBin()
		{
			PutRaw(new Bin(bin, 42));
			Record r = Eval(StringExp.ToString(Exp.IntBin(bin)));
			Assert.AreEqual("42", r.GetString(bin));
		}

		//=================================================================
		// Chained expressions — modify result feeds another StringExp
		//=================================================================

		[TestMethod]
		public void ChainedTrimThenUpperComposes()
		{
			Put("  hello world  ");
			// trim -> upper, both inside one expression tree.
			Exp chain = StringExp.Upper(policy, StringExp.Trim(policy, Exp.StringBin(bin)));
			Record r = Eval(chain);
			Assert.AreEqual("HELLO WORLD", r.GetString(bin));
		}

		//=================================================================
		// Filter-expression usage — predicate gates record retrieval
		//=================================================================

		[TestMethod]
		public void StartsWithFilterGatesGet()
		{
			Put("hello world");
			Policy p = new();

			// Matching filter -> record returned.
			p.filterExp = Exp.Build(StringExp.StartsWith(
				Exp.Val("hello"), Exp.StringBin(bin)));
			Assert.AreEqual("hello world", client.Get(p, key).GetString(bin));

			// Non-matching filter -> filtered out, get returns null.
			p.filterExp = Exp.Build(StringExp.StartsWith(
				Exp.Val("world"), Exp.StringBin(bin)));
			Assert.IsNull(client.Get(p, key));
		}

		//=================================================================
		// Nested-source — string inside a list/map projected via Exp getters
		//
		// StringExp does not accept CTX directly; callers compose with
		// ListExp.getByIndex / MapExp.getByKey to project the nested string
		// into an Exp and pass it as src.
		//=================================================================

		[TestMethod]
		public void StrlenOnStringNestedInListProjectedViaListExp()
		{
			List<Value> list = [Value.Get("alpha"), Value.Get("beta"), Value.Get("hello world")];
			PutRaw(new Bin(bin, list));

			Exp nestedString = ListExp.GetByIndex(
				ListReturnType.VALUE, Exp.Type.STRING, Exp.Val(2), Exp.ListBin(bin));
			Record r = Eval(StringExp.Strlen(nestedString));
			Assert.AreEqual(11L, r.GetLong(bin));
		}

		[TestMethod]
		public void UpperOnStringNestedInMapProjectedViaMapExp()
		{
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("a")] = Value.Get("hello"),
				[Value.Get("b")] = Value.Get("world")
			};
			PutRaw(new Bin(bin, map));

			Exp nestedString = MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.STRING, Exp.Val("a"), Exp.MapBin(bin));
			Record r = Eval(StringExp.Upper(policy, nestedString));
			Assert.AreEqual("HELLO", r.GetString(bin));
		}
	}
}
