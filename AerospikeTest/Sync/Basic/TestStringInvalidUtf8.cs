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
	/// Negative tests for the server's bin-UTF-8 entry gate 8.1.3.
    /// </summary>
	/// <para>
	/// Every read and modify op in {@link StringOperation} must reject a string
	/// bin whose stored bytes are not well-formed UTF-8. The server's
	/// {@code as_bin_string_read} / {@code as_bin_string_modify} entry helpers run
	/// {@code utf8_string_length} on the bin before dispatching to the op-specific
	/// code, returning {@code AS_ERR_INVALID_ENCODING} ({@link ResultCode#INVALID_ENCODING}).
	/// </para>
	/// <para>
	/// Csharp {@link String} cannot directly hold an ill-formed UTF-8 sequence —
	/// the standard {@code UTF_8} decoder substitutes {@code U+FFFD}. To plant
	/// raw invalid bytes in a string-typed bin we use
	/// {@link Value.BytesValue#BytesValue(byte[], int) Value.BytesValue(bytes, ParticleType.STRING)},
	/// which writes the bytes verbatim with the {@code STRING} particle type byte.
	/// </para>
	/// <para>
	/// The fixture {@code BAD = {0xED, 0xA0, 0x80}} is U+D800 (ill-formed surrogate),
	/// the same fixture used by the server's {@code EntryParityUtf8} unit tests.
	/// </para>
	[TestClass]
	public class TestStringInvalidUtf8 : TestSync
	{
		private static readonly string bin = "sbin";
		private static readonly Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "string-invalid-utf8-key");
		private static readonly StringPolicy policy = StringPolicy.Default;

		/** Ill-formed UTF-8: 3-byte encoding of U+D800 (surrogate). */
		private static readonly byte[] BAD = new byte[] { (byte)0xED, (byte)0xA0, (byte)0x80 };

		[ClassInitialize]
		public static void ServerVersionCheck(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "string operations");
		}

		[TestInitialize]
		public void PlantInvalidBin()
		{
			client.Delete(null, key);
			// BytesValue(..., ParticleType.STRING) writes the bytes verbatim under the
			// STRING particle type, bypassing Java-side UTF-8 sanitization.
			client.Put(null, key, new Bin(bin, new Value.BytesValue(BAD, ParticleType.STRING)));
		}

		private static void AssertInvalidEncoding(Operation op)
		{
			AerospikeException ae = Assert.Throws<AerospikeException>(() => client.Operate(null, key, op));
			Assert.AreEqual(ResultCode.INVALID_ENCODING, ae.Result);
		}

		//=================================================================
		// Read ops — bin gate fires before op-specific logic
		//=================================================================

		[TestMethod]
		public void StrlenRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Strlen(bin));
		}

		[TestMethod]
		public void SubstrRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Substr(bin, 0));
		}

		[TestMethod]
		public void CharAtRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.CharAt(bin, 0));
		}

		[TestMethod]
		public void FindRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Find(bin, "x"));
		}

		[TestMethod]
		public void ContainsRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Contains(bin, "x"));
		}

		[TestMethod]
		public void StartsWithRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.StartsWith(bin, "x"));
		}

		[TestMethod]
		public void EndsWithRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.EndsWith(bin, "x"));
		}

		[TestMethod]
		public void ToIntegerRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.ToInteger(bin));
		}

		[TestMethod]
		public void ToDoubleRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.ToDouble(bin));
		}

		// byte_length, to_blob, b64_decode, trim*, repeat, concat are listed in the
		// 8.1.3 client report as "unaffected" by UTF-8, but per the doc's §3 and
		// §11 they hit the same bin gate as strlen and must also reject.
		[TestMethod]
		public void ByteLengthRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.ByteLength(bin));
		}

		[TestMethod]
		public void IsNumericRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.IsNumeric(bin));
		}

		[TestMethod]
		public void IsUpperRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.IsUpper(bin));
		}

		[TestMethod]
		public void IsLowerRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.IsLower(bin));
		}

		[TestMethod]
		public void ToBlobRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.ToBlob(bin));
		}

		[TestMethod]
		public void SplitRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Split(bin, ","));
		}

		[TestMethod]
		public void B64DecodeRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.B64Decode(bin));
		}

		[TestMethod]
		public void RegexCompareRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.RegexCompare(bin, "x"));
		}

		//=================================================================
		// Modify ops — bin gate also fires here; bin must remain unchanged.
		//
		// We can't easily verify "bin bytes unchanged" via client.get because the
		// Csharp client decodes STRING particles through UTF-8, which
		// replaces ill-formed sequences with U+FFFD; the raw bytes are not
		// recoverable through the public client surface. The fact that a
		// subsequent strlen on the same bin still hits INVALID_ENCODING (see
		// failedModifyDoesNotOverwriteBin below) proves the failed modify did
		// not replace the bin with a well-formed value.
		//=================================================================

		[TestMethod]
		public void InsertRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Insert(policy, bin, 0, "x"));
		}

		[TestMethod]
		public void OverwriteRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Overwrite(policy, bin, 0, "x"));
		}

		[TestMethod]
		public void ConcatRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Concat(policy, bin, "x"));
		}

		[TestMethod]
		public void AppendRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Append(policy, bin, "x"));
		}

		[TestMethod]
		public void PrependRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Prepend(policy, bin, "x"));
		}

		[TestMethod]
		public void SnipRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Snip(policy, bin, 0, 1));
		}

		[TestMethod]
		public void ReplaceRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Replace(policy, bin, "x", "y"));
		}

		[TestMethod]
		public void ReplaceAllRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.ReplaceAll(policy, bin, "x", "y"));
		}

		[TestMethod]
		public void UpperRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Upper(policy, bin));
		}

		[TestMethod]
		public void LowerRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Lower(policy, bin));
		}

		[TestMethod]
		public void CaseFoldRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.CaseFold(policy, bin));
		}

		[TestMethod]
		public void NormalizeNFCRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.NormalizeNFC(policy, bin));
		}

		[TestMethod]
		public void TrimStartRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.TrimStart(policy, bin));
		}

		[TestMethod]
		public void TrimEndRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.TrimEnd(policy, bin));
		}

		[TestMethod]
		public void TrimRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Trim(policy, bin));
		}

		[TestMethod]
		public void PadStartRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.PadStart(policy, bin, 10, "*"));
		}

		[TestMethod]
		public void PadEndRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.PadEnd(policy, bin, 10, "*"));
		}

		[TestMethod]
		public void RepeatRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.Repeat(policy, bin, 2));
		}

		[TestMethod]
		public void RegexReplaceRejectsInvalidBin()
		{
			AssertInvalidEncoding(StringOperation.RegexReplace(
				policy, bin, "x", "y", StringRegexFlags.DEFAULT));
		}

		//=================================================================
		// Post-failure invariant
		//=================================================================

		[TestMethod]
		public void FailedModifyDoesNotOverwriteBin()
		{
			// First modify attempt must fail with INVALID_ENCODING.
			AssertInvalidEncoding(StringOperation.Upper(policy, bin));
			// A subsequent read on the same bin must also fail at the gate, proving
			// the bin still holds the original invalid bytes (the failed modify
			// did not replace it with a well-formed value).
			AssertInvalidEncoding(StringOperation.Strlen(bin));
		}

		//=================================================================
		// Client-side defense — invalid UTF-8 in an op argument is rejected
		// before the wire by Utf8.encodedLength (client/src/.../util/Utf8.java:87).
		// This complements the server's invalid-arg gate (which Java callers can't
		// normally reach because String → UTF-8 conversion either throws here or
		// substitutes well-formed bytes).
		//=================================================================

		[TestMethod]
		public void UnpairedSurrogateInArgIsRejectedClientSide()
		{
			client.Delete(null, key);
			client.Put(null, key, new Bin(bin, "hello"));
			// "\uD800" is an unpaired high surrogate. The client's UTF-8 encoder
			// throws AerospikeException before sending the op.
			AerospikeException ae = Assert.Throws<AerospikeException>(() => client.Operate(null, key,
				StringOperation.Find(bin, "\uD800")));
			// Sanity-check the message — encodedLength's throw includes the word.
			Assert.IsTrue(ae.Message != null && ae.Message.Contains("surrogate"));
		}
	}
}
