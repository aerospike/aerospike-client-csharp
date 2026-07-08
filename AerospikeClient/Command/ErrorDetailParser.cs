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
	/// Decoded server-supplied extended error detail (field
	/// <see cref="FieldType.ERROR_MESSAGE"/>): the formatted message, the numeric
	/// subcode, and (at verbosity 3 on an expression build failure) the structured
	/// <see cref="ExpressionTrace"/>.
	/// </summary>
	public readonly struct ErrorDetail(string message, int subCode, ExpressionTrace expTrace)
	{
		/// <summary>
		/// Formatted error message (may embed the subcode), or <c>null</c> when the
		/// server supplied neither a message nor a subcode.
		/// </summary>
		public string Message { get; } = message;

		/// <summary>
		/// Numeric subcode, or <see cref="Aerospike.Client.SubCode.NONE"/> when absent.
		/// </summary>
		public int SubCode { get; } = subCode;

		/// <summary>
		/// Structured expression trace, or <c>null</c> when absent.
		/// </summary>
		public ExpressionTrace ExpTrace { get; } = expTrace;
	}

	/// <summary>
	/// Decoder for the server-supplied extended error-detail payload (field
	/// <see cref="FieldType.ERROR_MESSAGE"/>, key 45).
	/// </summary>
	/// <remarks>
	/// Defensive by design: every element is optional, truncated or unknown data is
	/// tolerated, and a malformed payload never throws — a bad detail field must never
	/// mask the underlying error. The client's shared <see cref="Unpacker"/> is not
	/// reused here because it assumes an Aerospike particle-type prefix on strings and
	/// throws on unexpected/truncated input, neither of which suits this raw, best-effort
	/// error payload.
	/// </remarks>
	public static class ErrorDetailParser
	{
		/// <summary>
		/// Scan the response fields for the <see cref="FieldType.ERROR_MESSAGE"/> field and
		/// decode it, advancing <paramref name="offset"/> past every field.
		/// </summary>
		public static ErrorDetail ParseFields(byte[] buffer, ref int offset, int fieldCount)
		{
			ErrorDetail detail = default;

			for (int i = 0; i < fieldCount; i++)
			{
				int len = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;

				int type = buffer[offset++];
				int size = len - 1;

				if (type == FieldType.ERROR_MESSAGE && size > 0)
				{
					detail = Parse(buffer, offset, size);
				}
				offset += size;
			}
			return detail;
		}

		/// <summary>
		/// Parse the error-detail msgpack map. Map keys: 1 = subcode (uint),
		/// 2 = message (string), 3 = nested expression trace (verbosity 3).
		/// Returns <c>default</c> (empty detail) when the value is not a readable,
		/// non-empty map.
		/// </summary>
		public static ErrorDetail Parse(byte[] buffer, int offset, int size)
		{
			int end = offset + size;

			if (offset >= end)
			{
				return default;
			}

			// Read map header (fixmap, map16, map32).
			int b = buffer[offset++] & 0xFF;
			int count;

			if ((b & 0xF0) == 0x80)
			{
				count = b & 0x0F;
			}
			else if (b == 0xDE && offset + 2 <= end)
			{
				count = ByteUtil.BytesToShort(buffer, offset) & 0xFFFF;
				offset += 2;
			}
			else if (b == 0xDF && offset + 4 <= end)
			{
				count = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return default;
			}

			if (count <= 0)
			{
				return default;
			}

			string message = null;
			long subcode = -1;
			ExpressionTrace expTrace = null;

			for (int i = 0; i < count && offset < end; i++)
			{
				// Read key (positive fixint or uint8).
				int key;
				b = buffer[offset++] & 0xFF;

				if (b <= 0x7F)
				{
					key = b;
				}
				else if (b == 0xCC && offset < end)
				{
					key = buffer[offset++] & 0xFF;
				}
				else
				{
					break;
				}

				switch (key)
				{
					case 1: // AS_ERROR_DETAIL_KEY_SUBCODE
						subcode = UnpackUint(buffer, offset, end);
						offset = SkipMsgpackValue(buffer, offset, end);
						break;

					case 2: // AS_ERROR_DETAIL_KEY_MESSAGE
						(int Offset, int Length)? str = UnpackStr(buffer, offset, end);
						if (str.HasValue)
						{
							message = ByteUtil.Utf8ToString(buffer, str.Value.Offset, str.Value.Length);
							offset = str.Value.Offset + str.Value.Length;
						}
						else
						{
							offset = SkipMsgpackValue(buffer, offset, end);
						}
						break;

					case ExpressionTrace.AS_ERROR_DETAIL_KEY_EXP_TRACE: // nested expression-trace map (verbosity 3)
						expTrace = ParseExpTrace(buffer, offset, end);
						offset = SkipMsgpackValue(buffer, offset, end);
						break;

					default:
						offset = SkipMsgpackValue(buffer, offset, end);
						break;
				}
			}

			// The server only serializes subcodes >= 1 (SubCode.NONE = 0 is never sent),
			// so a parsed subcode always overrides the SubCode.NONE default.
			int resolvedSubCode = (subcode >= 0) ? (int)subcode : SubCode.NONE;
			string formatted;

			if (message != null && subcode >= 0)
			{
				formatted = message + " (subcode=" + subcode + ")";
			}
			else if (subcode >= 0)
			{
				formatted = "error subcode=" + subcode;
			}
			else
			{
				formatted = message;
			}

			return new ErrorDetail(formatted, resolvedSubCode, expTrace);
		}

		/// <summary>
		/// Parse the nested expression-trace map (top-level error-detail key 3, only sent
		/// at verbosity 3 on expression build-failure paths) into an <see cref="ExpressionTrace"/>.
		/// Treats every trace key as optional (never requires key 1 — build failures carry
		/// <see cref="SubCode.NONE"/>), skips unknown trace keys, tolerates the "..."
		/// path-truncation sentinel as an ordinary element, and never throws on a
		/// missing/truncated trace. An absent <c>lang</c> is left as -1 and surfaces as
		/// msgpack via <see cref="ExpressionTrace.Lang"/>. Returns <c>null</c> when the
		/// value is not a readable, non-empty map.
		/// </summary>
		private static ExpressionTrace ParseExpTrace(byte[] buffer, int offset, int end)
		{
			if (offset >= end)
			{
				return null;
			}

			// Read nested map header (fixmap, map16, map32).
			int b = buffer[offset++] & 0xFF;
			int count;

			if ((b & 0xF0) == 0x80)
			{
				count = b & 0x0F;
			}
			else if (b == 0xDE && offset + 2 <= end)
			{
				count = ByteUtil.BytesToShort(buffer, offset) & 0xFFFF;
				offset += 2;
			}
			else if (b == 0xDF && offset + 4 <= end)
			{
				count = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return null;
			}

			if (count <= 0)
			{
				return null;
			}

			int phase = -1;
			int byteOffset = -1;
			string op = null;
			int depth = -1;
			string[] path = null;
			string snippet = null;
			int lang = -1;
			int aelOffset = -1;
			int aelSpan = -1;

			for (int i = 0; i < count && offset < end; i++)
			{
				// Read key (positive fixint or uint8).
				int key;
				b = buffer[offset++] & 0xFF;

				if (b <= 0x7F)
				{
					key = b;
				}
				else if (b == 0xCC && offset < end)
				{
					key = buffer[offset++] & 0xFF;
				}
				else
				{
					break;
				}

				switch (key)
				{
					case ExpressionTrace.KEY_PHASE:
						phase = (int)UnpackUint(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_BYTE_OFFSET:
						byteOffset = (int)UnpackUint(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_OP:
						op = UnpackStrValue(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_DEPTH:
						depth = (int)UnpackUint(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_PATH:
						path = UnpackStrArray(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_SNIPPET:
						snippet = UnpackStrValue(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_LANG:
						lang = (int)UnpackUint(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_AEL_OFFSET:
						aelOffset = (int)UnpackUint(buffer, offset, end);
						break;
					case ExpressionTrace.KEY_AEL_SPAN:
						aelSpan = (int)UnpackUint(buffer, offset, end);
						break;
					default:
						// Unknown / reserved trace key (outcome, ael_line, ael_col, etc.) - skip.
						break;
				}

				// Advance past the value regardless of whether the key was recognized.
				offset = SkipMsgpackValue(buffer, offset, end);
			}

			return new ExpressionTrace(phase, byteOffset, op, depth, path, snippet, lang, aelOffset, aelSpan);
		}

		/// <summary>
		/// Unpack a msgpack string value, or <c>null</c> if the value at the offset is not
		/// a readable string.
		/// </summary>
		private static string UnpackStrValue(byte[] buffer, int offset, int end)
		{
			(int Offset, int Length)? r = UnpackStr(buffer, offset, end);
			return r.HasValue ? ByteUtil.Utf8ToString(buffer, r.Value.Offset, r.Value.Length) : null;
		}

		/// <summary>
		/// Unpack a msgpack array of strings (the expression-trace path). Preserves element
		/// order, keeps the "..." truncation sentinel as an ordinary element, and leaves a
		/// null slot for any element that is not a readable string. Returns <c>null</c> when
		/// the value is not a readable array.
		/// </summary>
		private static string[] UnpackStrArray(byte[] buffer, int offset, int end)
		{
			if (offset >= end)
			{
				return null;
			}

			int b = buffer[offset++] & 0xFF;
			int len;

			if ((b & 0xF0) == 0x90)
			{
				len = b & 0x0F;
			}
			else if (b == 0xDC && offset + 2 <= end)
			{
				len = ByteUtil.BytesToShort(buffer, offset) & 0xFFFF;
				offset += 2;
			}
			else if (b == 0xDD && offset + 4 <= end)
			{
				len = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return null;
			}

			if (len < 0)
			{
				return null;
			}

			string[] result = new string[len];

			for (int i = 0; i < len && offset < end; i++)
			{
				result[i] = UnpackStrValue(buffer, offset, end);
				offset = SkipMsgpackValue(buffer, offset, end);
			}
			return result;
		}

		/// <summary>
		/// Unpack a msgpack unsigned integer value. Returns -1 on failure.
		/// </summary>
		private static long UnpackUint(byte[] buffer, int offset, int end)
		{
			if (offset >= end)
			{
				return -1;
			}

			int b = buffer[offset] & 0xFF;

			if (b <= 0x7F)
			{
				return b;
			}
			else if (b == 0xCC && offset + 1 < end)
			{
				return buffer[offset + 1] & 0xFF;
			}
			else if (b == 0xCD && offset + 2 < end)
			{
				return ByteUtil.BytesToShort(buffer, offset + 1) & 0xFFFF;
			}
			else if (b == 0xCE && offset + 4 < end)
			{
				return ByteUtil.BytesToInt(buffer, offset + 1) & 0xFFFFFFFFL;
			}
			else if (b == 0xCF && offset + 8 < end)
			{
				return ByteUtil.BytesToLong(buffer, offset + 1);
			}
			return -1;
		}

		/// <summary>
		/// Unpack a msgpack string. Returns its (offset, length) or <c>null</c> on failure.
		/// </summary>
		private static (int Offset, int Length)? UnpackStr(byte[] buffer, int offset, int end)
		{
			if (offset >= end)
			{
				return null;
			}

			int b = buffer[offset++] & 0xFF;
			int len;

			if ((b & 0xE0) == 0xA0)
			{
				len = b & 0x1F;
			}
			else if (b == 0xD9 && offset < end)
			{
				len = buffer[offset++] & 0xFF;
			}
			else if (b == 0xDA && offset + 1 < end)
			{
				len = ByteUtil.BytesToShort(buffer, offset) & 0xFFFF;
				offset += 2;
			}
			else if (b == 0xDB && offset + 3 < end)
			{
				len = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return null;
			}

			if (len < 0 || offset + len > end)
			{
				return null;
			}

			return (offset, len);
		}

		/// <summary>
		/// Skip a single msgpack value, returning the new offset.
		/// </summary>
		private static int SkipMsgpackValue(byte[] buffer, int offset, int end)
		{
			if (offset >= end)
			{
				return end;
			}

			int b = buffer[offset++] & 0xFF;

			// Positive fixint / negative fixint
			if (b <= 0x7F || b >= 0xE0)
			{
				return offset;
			}
			// fixstr
			if ((b & 0xE0) == 0xA0)
			{
				return offset + (b & 0x1F);
			}
			// fixmap
			if ((b & 0xF0) == 0x80)
			{
				int count = (b & 0x0F) * 2;
				for (int i = 0; i < count && offset < end; i++)
				{
					offset = SkipMsgpackValue(buffer, offset, end);
				}
				return offset;
			}
			// fixarray
			if ((b & 0xF0) == 0x90)
			{
				int count = b & 0x0F;
				for (int i = 0; i < count && offset < end; i++)
				{
					offset = SkipMsgpackValue(buffer, offset, end);
				}
				return offset;
			}

			switch (b)
			{
				case 0xC0: // nil
				case 0xC2: // false
				case 0xC3: // true
					return offset;
				case 0xCC: // uint8
				case 0xD0: // int8
					return offset + 1;
				case 0xCD: // uint16
				case 0xD1: // int16
					return offset + 2;
				case 0xCE: // uint32
				case 0xD2: // int32
				case 0xCA: // float32
					return offset + 4;
				case 0xCF: // uint64
				case 0xD3: // int64
				case 0xCB: // float64
					return offset + 8;
				case 0xD9: // str8
				case 0xC4: // bin8
					if (offset < end)
					{
						return offset + 1 + (buffer[offset] & 0xFF);
					}
					return end;
				case 0xDA: // str16
				case 0xC5: // bin16
					if (offset + 1 < end)
					{
						return offset + 2 + (ByteUtil.BytesToShort(buffer, offset) & 0xFFFF);
					}
					return end;
				case 0xDB: // str32
				case 0xC6: // bin32
					if (offset + 3 < end)
					{
						return offset + 4 + ByteUtil.BytesToInt(buffer, offset);
					}
					return end;
				case 0xDC: // array16
				case 0xDE:
					{ // map16
						if (offset + 1 >= end)
						{
							return end;
						}
						int count = (ByteUtil.BytesToShort(buffer, offset) & 0xFFFF) * ((b == 0xDE) ? 2 : 1);
						offset += 2;
						for (int i = 0; i < count && offset < end; i++)
						{
							offset = SkipMsgpackValue(buffer, offset, end);
						}
						return offset;
					}
				case 0xDD: // array32
				case 0xDF:
					{ // map32
						if (offset + 3 >= end)
						{
							return end;
						}
						int count = ByteUtil.BytesToInt(buffer, offset) * ((b == 0xDF) ? 2 : 1);
						offset += 4;
						for (int i = 0; i < count && offset < end; i++)
						{
							offset = SkipMsgpackValue(buffer, offset, end);
						}
						return offset;
					}
				default:
					return end;
			}
		}
	}
}
