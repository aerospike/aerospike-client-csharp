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
using System.Reflection;
using System.Text;

namespace Aerospike.Test
{
	[TestClass]
	public class TestErrorDetailParser
	{
		[TestMethod]
		public void VerbosityShiftAndMaskAreConsistent()
		{
			Assert.AreEqual(5, Command.INFO4_ERROR_VERBOSITY_SHIFT);
			Assert.AreEqual(0x60, Command.INFO4_ERROR_VERBOSITY_MASK);
			Assert.AreEqual(0x60, 0x03 << Command.INFO4_ERROR_VERBOSITY_SHIFT);
		}

		[TestMethod]
		public void VerbosityValueInRangeIsPreservedAfterMasking()
		{
			for (int v = 0; v <= 3; v++)
			{
				int actual = EncodeErrorVerbosity(v);
				Assert.AreEqual(v << Command.INFO4_ERROR_VERBOSITY_SHIFT, actual, "v=" + v);
			}
		}

		[TestMethod]
		public void VerbosityOutOfRangeIsClamped()
		{
			Assert.AreEqual(0, EncodeErrorVerbosity(-1));
			Assert.AreEqual(0, EncodeErrorVerbosity(int.MinValue));
			Assert.AreEqual(0x60, EncodeErrorVerbosity(4));
			Assert.AreEqual(0x60, EncodeErrorVerbosity(int.MaxValue));
		}

		[TestMethod]
		public void ParsesFixmapWithSubcodeAndMessage()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(99)),
				Pair(IntKey(2), FixStr("cannot append"))
			);

			AssertParsed(detail, "cannot append", 99);
		}

		[TestMethod]
		public void ParsesFixmapWithSubcodeOnly()
		{
			byte[] detail = FixMap(Pair(IntKey(1), FixInt(42)));

			AssertParsed(detail, null, 42);
		}

		[TestMethod]
		public void ParsesFixmapWithMessageOnly()
		{
			byte[] detail = FixMap(Pair(IntKey(2), FixStr("oops")));

			Assert.AreEqual("oops", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesKeysInReverseOrder()
		{
			byte[] detail = FixMap(
				Pair(IntKey(2), FixStr("swap")),
				Pair(IntKey(1), FixInt(7))
			);

			AssertParsed(detail, "swap", 7);
		}

		[TestMethod]
		public void ParsesMap16Header()
		{
			List<byte> payload = new()
			{
				0xDE,
				0x00,
				16
			};
			payload.AddRange(Pair(IntKey(1), FixInt(7)));
			payload.AddRange(Pair(IntKey(2), FixStr("boom")));

			for (int i = 0; i < 14; i++)
			{
				payload.Add(0xCC);
				payload.Add((byte)(100 + i));
				payload.Add(0xC0);
			}

			AssertParsed(payload.ToArray(), "boom", 7);
		}

		[TestMethod]
		public void ParsesMap32Header()
		{
			List<byte> payload = new() { 0xDF };
			WriteInt(payload, 2);
			payload.AddRange(Pair(IntKey(1), FixInt(9)));
			payload.AddRange(Pair(IntKey(2), FixStr("m32")));

			AssertParsed(payload.ToArray(), "m32", 9);
		}

		[TestMethod]
		public void ParsesSubcodeAsUint8()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCC, 200)),
				Pair(IntKey(2), FixStr("u8"))
			);

			AssertParsed(detail, "u8", 200);
		}

		[TestMethod]
		public void ParsesSubcodeAsUint16()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCD, 0x04, 0x4C)),
				Pair(IntKey(2), FixStr("hi"))
			);

			AssertParsed(detail, "hi", 1100);
		}

		[TestMethod]
		public void ParsesSubcodeAsUint32()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCE, 0x00, 0x01, 0x11, 0x70)),
				Pair(IntKey(2), FixStr("x"))
			);

			AssertParsed(detail, "x", 70000);
		}

		[TestMethod]
		public void ParsesSubcodeAsUint64()
		{
			const long value = 5_000_000_000L;
			List<byte> subcode = new() { 0xCF };
			WriteLong(subcode, value);

			byte[] detail = FixMap(
				Pair(IntKey(1), subcode.ToArray()),
				Pair(IntKey(2), FixStr("u64"))
			);

			AssertParsed(detail, "u64", unchecked((int)value));
		}

		[TestMethod]
		public void ParsesMessageAsStr8()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(3)),
				Pair(IntKey(2), Str8("string8"))
			);

			AssertParsed(detail, "string8", 3);
		}

		[TestMethod]
		public void ParsesMessageAsStr16()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(4)),
				Pair(IntKey(2), Str16("string16"))
			);

			AssertParsed(detail, "string16", 4);
		}

		[TestMethod]
		public void ParsesMessageAsStr32()
		{
			string message = new('x', 100);
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(5)),
				Pair(IntKey(2), Str32(message))
			);

			AssertParsed(detail, message, 5);
		}

		[TestMethod]
		public void EmptyMapProducesNoMessage()
		{
			Assert.IsNull(ParseErrorField(Bytes(0x80)));
		}

		[TestMethod]
		public void TruncatedValueReturnsNullNotThrow()
		{
			Assert.IsNull(ParseErrorField(Bytes(0x81, 0x01, 0xCD)));
		}

		[TestMethod]
		public void TruncatedMapHeaderReturnsNull()
		{
			Assert.IsNull(ParseErrorField(Bytes(0xDE)));
		}

		[TestMethod]
		public void UnknownKeysAreSkipped()
		{
			byte[] detail = FixMap(
				Pair(IntKey(50), FixInt(0)),
				Pair(IntKey(1), FixInt(7)),
				Pair(IntKey(51), Bytes(0xC0)),
				Pair(IntKey(2), FixStr("z"))
			);

			AssertParsed(detail, "z", 7);
		}

		[TestMethod]
		public void ParseSucceedsWhenAdditionalNonErrorFieldsPresent()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(1)),
				Pair(IntKey(2), FixStr("ok"))
			);
			byte[] fields = Fields(
				(0xCD, Bytes(0x01, 0x02, 0x03)),
				(FieldType.ERROR_MESSAGE, detail)
			);

			AssertParsedFields(fields, 2, "ok", 1);
		}

		[TestMethod]
		public void MissingErrorFieldYieldsNullMessage()
		{
			Assert.IsNull(ParseFields(Array.Empty<byte>(), 0));
		}

		// ---------- Parser: verbosity-3 expression trace (nested key-3 map) ----------

		[TestMethod]
		public void ParsesFullExpressionTrace()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(7)),
				Pair(IntKey(ExpressionTrace.KEY_OP), FixStr("cmp_eq")),
				Pair(IntKey(ExpressionTrace.KEY_DEPTH), FixInt(3)),
				Pair(IntKey(ExpressionTrace.KEY_PATH), FixArray(FixStr("and"), FixStr("eq"), FixStr("cmp_eq"))),
				Pair(IntKey(ExpressionTrace.KEY_SNIPPET), FixStr("eq(int,float)"))
			);
			byte[] detail = FixMap(
				Pair(IntKey(2), FixStr("bad exp")),
				Pair(IntKey(3), trace)
			);

			ErrorDetail command = ParseDetail(detail);

			Assert.AreEqual("bad exp", command.Message);
			ExpressionTrace traceResult = command.ExpTrace;
			Assert.IsNotNull(traceResult, "Expected a parsed expression trace");
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, traceResult.Phase);
			Assert.AreEqual(7, traceResult.ByteOffset);
			Assert.AreEqual("cmp_eq", traceResult.Op);
			Assert.AreEqual(3, traceResult.Depth);
			Assert.AreEqual("eq(int,float)", traceResult.Snippet);
			CollectionAssert.AreEqual(new[] { "and", "eq", "cmp_eq" }, traceResult.Path);
			Assert.AreEqual(ExpressionTrace.LANG_MSGPACK, traceResult.Lang);
		}

		[TestMethod]
		public void ParsesTracePathTruncationSentinel()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_DEPTH), FixInt(20)),
				Pair(IntKey(ExpressionTrace.KEY_PATH),
					FixArray(FixStr("and"), FixStr("or"), FixStr("..."), FixStr("cmp_eq")))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseDetail(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(20, traceResult.Depth, "Depth reports the true count, not the truncated path length");
			Assert.IsNotNull(traceResult.Path);
			Assert.AreEqual(4, traceResult.Path.Length);
			Assert.AreEqual(ExpressionTrace.PATH_TRUNCATION_SENTINEL, traceResult.Path[2]);
			Assert.AreEqual("and", traceResult.Path[0]);
			Assert.AreEqual("cmp_eq", traceResult.Path[3]);
		}

		[TestMethod]
		public void ParsesTraceWithSnippetAndPathAbsent()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(12)),
				Pair(IntKey(ExpressionTrace.KEY_OP), FixStr("add")),
				Pair(IntKey(ExpressionTrace.KEY_DEPTH), FixInt(2))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseDetail(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, traceResult.Phase);
			Assert.AreEqual(12, traceResult.ByteOffset);
			Assert.AreEqual("add", traceResult.Op);
			Assert.AreEqual(2, traceResult.Depth);
			Assert.IsNull(traceResult.Snippet, "Snippet absent within a present trace");
			Assert.IsNull(traceResult.Path, "Path absent within a present trace");
		}

		[TestMethod]
		public void ParsesTraceSkippingUnknownTraceKeys()
		{
			// Reserved keys 11/12 and unknown key 99 must not disturb known fields.
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_LINE), FixInt(9)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(4)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_COL), FixInt(2)),
				Pair(IntKey(99), FixStr("ignored"))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseDetail(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, traceResult.Phase);
			Assert.AreEqual(4, traceResult.ByteOffset);
			Assert.IsNull(traceResult.Op);
			Assert.AreEqual(-1, traceResult.Depth);
		}

		[TestMethod]
		public void ParsesTraceMaxLengthPath()
		{
			// The server caps the path at 16 frames. The "..." sentinel is an
			// additional element, so the true maximum is 17 and requires array16.
			byte[][] elements = new byte[17][];

			for (int i = 0; i < 15; i++)
			{
				elements[i] = FixStr("and");
			}
			elements[15] = FixStr(ExpressionTrace.PATH_TRUNCATION_SENTINEL);
			elements[16] = FixStr("eq");

			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_DEPTH), FixInt(40)),
				Pair(IntKey(ExpressionTrace.KEY_PATH), Array16(elements))
			);

			ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(40, traceResult.Depth);
			Assert.AreEqual(17, traceResult.Path.Length);
			Assert.AreEqual(ExpressionTrace.PATH_TRUNCATION_SENTINEL, traceResult.Path[15]);
			Assert.AreEqual("eq", traceResult.Path[16]);
		}

		[TestMethod]
		public void ParsesTraceOutcomeAndOperands()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_EVAL)),
				Pair(IntKey(ExpressionTrace.KEY_OP), FixStr("cmp_gt")),
				Pair(IntKey(ExpressionTrace.KEY_OUTCOME), FixInt(ExpressionTrace.OUTCOME_FALSE)),
				Pair(IntKey(ExpressionTrace.KEY_OPERANDS), FixArray(FixStr("15"), FixStr("18")))
			);

			ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.PHASE_EVAL, traceResult.Phase);
			Assert.AreEqual(ExpressionTrace.OUTCOME_FALSE, traceResult.Outcome);
			CollectionAssert.AreEqual(new[] { "15", "18" }, traceResult.Operands);
		}

		[TestMethod]
		public void ParsesTraceOperandsAlreadyClippedByServer()
		{
			// The server may clip long string operands before staging them. The client
			// must pass the rendered values through without truncating further.
			string lhs = new('a', 48);
			string rhs = new('b', 48);

			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_EVAL)),
				Pair(IntKey(ExpressionTrace.KEY_OUTCOME), FixInt(ExpressionTrace.OUTCOME_FALSE)),
				Pair(IntKey(ExpressionTrace.KEY_OPERANDS), FixArray(Str8(lhs), Str8(rhs)))
			);

			ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

			Assert.IsNotNull(traceResult);
			CollectionAssert.AreEqual(new[] { lhs, rhs }, traceResult.Operands);
		}

		[TestMethod]
		public void ParsesTraceOutcomesWithoutOperands()
		{
			foreach (int outcome in new[] { ExpressionTrace.OUTCOME_FAULT, ExpressionTrace.OUTCOME_ABSENT })
			{
				byte[] trace = FixMap(
					Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_EVAL)),
					Pair(IntKey(ExpressionTrace.KEY_OUTCOME), FixInt(outcome))
				);

				ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

				Assert.IsNotNull(traceResult);
				Assert.AreEqual(outcome, traceResult.Outcome);
				Assert.IsNull(traceResult.Operands);
			}
		}

		[TestMethod]
		public void ParsesFalseOutcomeWithOperandsDropped()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_EVAL)),
				Pair(IntKey(ExpressionTrace.KEY_OUTCOME), FixInt(ExpressionTrace.OUTCOME_FALSE))
			);

			ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.OUTCOME_FALSE, traceResult.Outcome);
			Assert.IsNull(traceResult.Operands);
		}

		[TestMethod]
		public void ParsesBuildTraceWithoutOutcomeOrOperands()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(3))
			);

			ExpressionTrace traceResult = ParseDetail(FixMap(Pair(IntKey(3), trace))).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(-1, traceResult.Outcome);
			Assert.IsNull(traceResult.Operands);
		}

		[TestMethod]
		public void ParsesTraceLangAbsentIsMsgpack()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(1))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseDetail(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.LANG_MSGPACK, traceResult.Lang, "Absent lang must be treated as msgpack");
			Assert.AreEqual(-1, traceResult.AelOffset);
			Assert.AreEqual(-1, traceResult.AelSpan);
		}

		[TestMethod]
		public void ParsesTraceLangAelWithOffsets()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_LANG), FixInt(ExpressionTrace.LANG_AEL)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_OFFSET), FixInt(42)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_SPAN), FixInt(6))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseDetail(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.LANG_AEL, traceResult.Lang);
			Assert.AreEqual(42, traceResult.AelOffset);
			Assert.AreEqual(6, traceResult.AelSpan);
		}

		[TestMethod]
		public void NoKey3YieldsNoTrace()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(4)),
				Pair(IntKey(2), FixStr("plain"))
			);

			ErrorDetail command = ParseDetail(detail);

			Assert.AreEqual("plain", command.Message);
			Assert.AreEqual(4, command.SubCode);
			Assert.IsNull(command.ExpTrace, "No key 3 should yield no expression trace");
		}

		[TestMethod]
		public void MessageStillSurfacesAlongsideTraceRegardlessOfKeyOrder()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_OP), FixStr("eq"))
			);
			byte[] detail = FixMap(
				Pair(IntKey(3), trace),
				Pair(IntKey(2), FixStr("bad exp"))
			);

			ErrorDetail command = ParseDetail(detail);

			Assert.AreEqual("bad exp", command.Message);
			Assert.IsNotNull(command.ExpTrace);
			Assert.AreEqual("eq", command.ExpTrace.Op);
		}

		[TestMethod]
		public void EmptyTraceMapYieldsNoTrace()
		{
			byte[] detail = FixMap(Pair(IntKey(3), FixMap()));

			Assert.IsNull(ParseDetail(detail).ExpTrace);
		}

		private static void AssertParsed(byte[] detail, string expectedMessage, int expectedSubCode)
		{
			ErrorDetail parsed = ParseDetail(detail);
			Assert.AreEqual(expectedMessage, parsed.Message);
			Assert.AreEqual(expectedSubCode, parsed.SubCode);
		}

		private static void AssertParsedFields(byte[] fields, int fieldCount, string expectedMessage, int expectedSubCode)
		{
			int offset = 0;
			ErrorDetail parsed = ErrorDetailParser.ParseFields(fields, ref offset, fieldCount);
			Assert.AreEqual(expectedMessage, parsed.Message);
			Assert.AreEqual(expectedSubCode, parsed.SubCode);
		}

		private static string ParseErrorField(byte[] msgpackDetail)
		{
			return ParseDetail(msgpackDetail).Message;
		}

		private static int EncodeErrorVerbosity(int verbosity)
		{
			MethodInfo method = typeof(Command).GetMethod(
				"ErrorVerbosityBits",
				BindingFlags.Static | BindingFlags.NonPublic);
			Assert.IsNotNull(method);
			return (int)method.Invoke(null, [verbosity]);
		}

		private static string ParseFields(byte[] fields, int fieldCount)
		{
			int offset = 0;
			return ErrorDetailParser.ParseFields(fields, ref offset, fieldCount).Message;
		}

		private static ErrorDetail ParseDetail(byte[] msgpackDetail)
		{
			int offset = 0;
			return ErrorDetailParser.ParseFields(Fields((FieldType.ERROR_MESSAGE, msgpackDetail)), ref offset, 1);
		}

		private static byte[] Fields(params (int Type, byte[] Data)[] fields)
		{
			List<byte> bytes = new();

			foreach ((int type, byte[] data) in fields)
			{
				WriteInt(bytes, data.Length + 1);
				bytes.Add((byte)type);
				bytes.AddRange(data);
			}
			return bytes.ToArray();
		}

		private static byte[] FixArray(params byte[][] elements)
		{
			Assert.IsTrue(elements.Length <= 15);
			List<byte> bytes = new() { (byte)(0x90 | elements.Length) };

			foreach (byte[] element in elements)
			{
				bytes.AddRange(element);
			}
			return bytes.ToArray();
		}

		private static byte[] Array16(params byte[][] elements)
		{
			List<byte> bytes = new() { 0xDC };
			WriteShort(bytes, elements.Length);

			foreach (byte[] element in elements)
			{
				bytes.AddRange(element);
			}
			return bytes.ToArray();
		}

		private static byte[] FixMap(params byte[][] pairs)
		{
			Assert.IsTrue(pairs.Length <= 15);
			List<byte> bytes = new() { (byte)(0x80 | pairs.Length) };

			foreach (byte[] pair in pairs)
			{
				bytes.AddRange(pair);
			}
			return bytes.ToArray();
		}

		private static byte[] Pair(byte[] key, byte[] value)
		{
			return key.Concat(value).ToArray();
		}

		private static byte[] IntKey(int value)
		{
			Assert.IsTrue(value >= 0 && value <= 0x7F);
			return Bytes(value);
		}

		private static byte[] FixInt(int value)
		{
			Assert.IsTrue(value >= 0 && value <= 0x7F);
			return Bytes(value);
		}

		private static byte[] FixStr(string value)
		{
			byte[] data = Encoding.UTF8.GetBytes(value);
			Assert.IsTrue(data.Length <= 31);

			return Bytes(0xA0 | data.Length).Concat(data).ToArray();
		}

		private static byte[] Str8(string value)
		{
			byte[] data = Encoding.UTF8.GetBytes(value);
			Assert.IsTrue(data.Length <= byte.MaxValue);

			return Bytes(0xD9, data.Length).Concat(data).ToArray();
		}

		private static byte[] Str16(string value)
		{
			byte[] data = Encoding.UTF8.GetBytes(value);
			List<byte> bytes = new() { 0xDA };
			WriteShort(bytes, data.Length);
			bytes.AddRange(data);
			return bytes.ToArray();
		}

		private static byte[] Str32(string value)
		{
			byte[] data = Encoding.UTF8.GetBytes(value);
			List<byte> bytes = new() { 0xDB };
			WriteInt(bytes, data.Length);
			bytes.AddRange(data);
			return bytes.ToArray();
		}

		private static byte[] Bytes(params int[] values)
		{
			return values.Select(v => (byte)v).ToArray();
		}

		private static void WriteShort(List<byte> bytes, int value)
		{
			bytes.Add((byte)((value >> 8) & 0xFF));
			bytes.Add((byte)(value & 0xFF));
		}

		private static void WriteInt(List<byte> bytes, int value)
		{
			bytes.Add((byte)((value >> 24) & 0xFF));
			bytes.Add((byte)((value >> 16) & 0xFF));
			bytes.Add((byte)((value >> 8) & 0xFF));
			bytes.Add((byte)(value & 0xFF));
		}

		private static void WriteLong(List<byte> bytes, long value)
		{
			bytes.Add((byte)((value >> 56) & 0xFF));
			bytes.Add((byte)((value >> 48) & 0xFF));
			bytes.Add((byte)((value >> 40) & 0xFF));
			bytes.Add((byte)((value >> 32) & 0xFF));
			bytes.Add((byte)((value >> 24) & 0xFF));
			bytes.Add((byte)((value >> 16) & 0xFF));
			bytes.Add((byte)((value >> 8) & 0xFF));
			bytes.Add((byte)(value & 0xFF));
		}
	}
}
