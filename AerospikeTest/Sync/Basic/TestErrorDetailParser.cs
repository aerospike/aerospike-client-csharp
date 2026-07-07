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

		[TestInitialize()]
		public void CheckServerVersion()
		{
			//CheckServerVersion(Node.SERVER_VERSION_8_1_3, "extended errors");
		}

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
				int actual = (v << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK;
				Assert.AreEqual(v << Command.INFO4_ERROR_VERBOSITY_SHIFT, actual, "v=" + v);
			}
		}

		[TestMethod]
		public void VerbosityOutOfRangeCannotCorruptOtherInfo4Bits()
		{
			int otherBits = ~Command.INFO4_ERROR_VERBOSITY_MASK & 0xFF;

			foreach (int v in new[] { 0, 1, 2, 3, 4, 8, 16, 255, int.MaxValue, -1 })
			{
				int written = (v << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK;
				Assert.AreEqual(0, written & otherBits, "v=" + v);
				Assert.AreEqual(written, written & Command.INFO4_ERROR_VERBOSITY_MASK, "v=" + v);
			}

			Assert.AreEqual(0, (4 << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
			Assert.AreEqual(0, (8 << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
			Assert.AreEqual(0, (16 << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
		}

		[TestMethod]
		public void ParsesFixmapWithSubcodeAndMessage()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(99)),
				Pair(IntKey(2), FixStr("cannot append"))
			);

			Assert.AreEqual("cannot append (subcode=99)", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesFixmapWithSubcodeOnly()
		{
			byte[] detail = FixMap(Pair(IntKey(1), FixInt(42)));

			Assert.AreEqual("error subcode=42", ParseErrorField(detail));
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

			Assert.AreEqual("swap (subcode=7)", ParseErrorField(detail));
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

			Assert.AreEqual("boom (subcode=7)", ParseErrorField(payload.ToArray()));
		}

		[TestMethod]
		public void ParsesMap32Header()
		{
			List<byte> payload = new() { 0xDF };
			WriteInt(payload, 2);
			payload.AddRange(Pair(IntKey(1), FixInt(9)));
			payload.AddRange(Pair(IntKey(2), FixStr("m32")));

			Assert.AreEqual("m32 (subcode=9)", ParseErrorField(payload.ToArray()));
		}

		[TestMethod]
		public void ParsesSubcodeAsUint8()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCC, 200)),
				Pair(IntKey(2), FixStr("u8"))
			);

			Assert.AreEqual("u8 (subcode=200)", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesSubcodeAsUint16()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCD, 0x04, 0x4C)),
				Pair(IntKey(2), FixStr("hi"))
			);

			Assert.AreEqual("hi (subcode=1100)", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesSubcodeAsUint32()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), Bytes(0xCE, 0x00, 0x01, 0x11, 0x70)),
				Pair(IntKey(2), FixStr("x"))
			);

			Assert.AreEqual("x (subcode=70000)", ParseErrorField(detail));
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

			Assert.AreEqual("u64 (subcode=" + value + ")", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesMessageAsStr8()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(3)),
				Pair(IntKey(2), Str8("string8"))
			);

			Assert.AreEqual("string8 (subcode=3)", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesMessageAsStr16()
		{
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(4)),
				Pair(IntKey(2), Str16("string16"))
			);

			Assert.AreEqual("string16 (subcode=4)", ParseErrorField(detail));
		}

		[TestMethod]
		public void ParsesMessageAsStr32()
		{
			string message = new('x', 100);
			byte[] detail = FixMap(
				Pair(IntKey(1), FixInt(5)),
				Pair(IntKey(2), Str32(message))
			);

			Assert.AreEqual(message + " (subcode=5)", ParseErrorField(detail));
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

			Assert.AreEqual("z (subcode=7)", ParseErrorField(detail));
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

			Assert.AreEqual("ok (subcode=1)", ParseFields(fields, 2));
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

			TestCommand command = ParseCommand(detail);

			Assert.AreEqual("bad exp", command.ServerMessage);
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

			ExpressionTrace traceResult = ParseCommand(detail).ExpTrace;

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

			ExpressionTrace traceResult = ParseCommand(detail).ExpTrace;

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
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_OUTCOME), FixInt(5)),
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_LINE), FixInt(9)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(4)),
				Pair(IntKey(ExpressionTrace.KEY_AEL_COL), FixInt(2)),
				Pair(IntKey(99), FixStr("ignored"))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseCommand(detail).ExpTrace;

			Assert.IsNotNull(traceResult);
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, traceResult.Phase);
			Assert.AreEqual(4, traceResult.ByteOffset);
			Assert.IsNull(traceResult.Op);
			Assert.AreEqual(-1, traceResult.Depth);
		}

		[TestMethod]
		public void ParsesTraceLangAbsentIsMsgpack()
		{
			byte[] trace = FixMap(
				Pair(IntKey(ExpressionTrace.KEY_PHASE), FixInt(ExpressionTrace.PHASE_BUILD)),
				Pair(IntKey(ExpressionTrace.KEY_BYTE_OFFSET), FixInt(1))
			);
			byte[] detail = FixMap(Pair(IntKey(3), trace));

			ExpressionTrace traceResult = ParseCommand(detail).ExpTrace;

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

			ExpressionTrace traceResult = ParseCommand(detail).ExpTrace;

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

			TestCommand command = ParseCommand(detail);

			Assert.AreEqual("plain (subcode=4)", command.ServerMessage);
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

			TestCommand command = ParseCommand(detail);

			Assert.AreEqual("bad exp", command.ServerMessage);
			Assert.IsNotNull(command.ExpTrace);
			Assert.AreEqual("eq", command.ExpTrace.Op);
		}

		[TestMethod]
		public void EmptyTraceMapYieldsNoTrace()
		{
			byte[] detail = FixMap(Pair(IntKey(3), FixMap()));

			Assert.IsNull(ParseCommand(detail).ExpTrace);
		}

		private static string ParseErrorField(byte[] msgpackDetail)
		{
			return ParseCommand(msgpackDetail).ServerMessage;
		}

		private static string ParseFields(byte[] fields, int fieldCount)
		{
			TestCommand command = new();
			command.ParseFieldsForTest(fields, fieldCount);
			return command.ServerMessage;
		}

		private static TestCommand ParseCommand(byte[] msgpackDetail)
		{
			TestCommand command = new();
			command.ParseFieldsForTest(Fields((FieldType.ERROR_MESSAGE, msgpackDetail)), 1);
			return command;
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

		private sealed class TestCommand : Command
		{
			private static readonly FieldInfo DataBufferField = typeof(Command).GetField(
				"dataBuffer",
				BindingFlags.Instance | BindingFlags.NonPublic);

			private static readonly FieldInfo DataOffsetField = typeof(Command).GetField(
				"dataOffset",
				BindingFlags.Instance | BindingFlags.NonPublic);

			public TestCommand()
				: base(0, 0, 0)
			{
			}

			public string ServerMessage => serverMessage;

			public ExpressionTrace ExpTrace => expTrace;

			public void ParseFieldsForTest(byte[] fields, int count)
			{
				serverMessage = null;
				expTrace = null;
				fieldCount = count;
				DataBufferField.SetValue(this, fields);
				DataOffsetField.SetValue(this, 0);

				ParseFields(null, null, false);
			}

			protected override int SizeBuffer()
			{
				throw new NotSupportedException();
			}

			protected override void End()
			{
				throw new NotSupportedException();
			}

			protected override void SetLength(int length)
			{
				throw new NotSupportedException();
			}
		}
	}
}
