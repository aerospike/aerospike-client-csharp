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

using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Structured expression build/eval trace surfaced at error-detail verbosity 3.
	/// </summary>
	/// <remarks>
	/// <para>
	/// When extended error detail is requested at verbosity 3 (see
	/// <see cref="Policy.errorDetailVerbosity"/>) and the server fails to process an
	/// expression, it can attach this trace as a nested map under the field-45
	/// error-detail key <see cref="AS_ERROR_DETAIL_KEY_EXP_TRACE"/>. This trace is
	/// surfaced on <see cref="AerospikeException.ExpTrace"/>.
	/// </para>
	/// <para>
	/// Build traces identify where decoding failed. Evaluation traces can additionally
	/// identify whether evaluation faulted, returned false, or referenced absent data,
	/// and may include the decisive comparison operands. The trace is purely additive
	/// diagnostic detail — it never changes the result code, subcode, or message format.
	/// </para>
	/// <para>
	/// <b>Every field is optional.</b> The server caps the whole error-detail payload
	/// and drops optional context such as <c>operands</c>, <c>snippet</c>, and
	/// <c>path</c> when the budget is tight, so those may be absent even within a
	/// present trace. Absent integer fields read as <c>-1</c> (except
	/// <see cref="Lang"/>, which defaults to
	/// <see cref="LANG_MSGPACK"/>); absent object fields read as <c>null</c>.
	/// Never require any field.
	/// </para>
	/// <para>
	/// <b>Two coordinate spaces — do not conflate them.</b> <see cref="ByteOffset"/> is a
	/// byte offset into the <i>msgpack expression payload</i> the client sent. The
	/// <see cref="AelOffset"/>/<see cref="AelSpan"/> pair are offsets into <i>AEL source
	/// text</i> — a different coordinate space.
	/// </para>
	/// <para>
	/// The nested-map key/value constants below mirror the server's <c>proto.h</c> names
	/// so they stay greppable across repositories. They are append-only.
	/// </para>
	/// <para>Construct a trace. Use <c>-1</c> / <c>null</c> for any absent field.</para>
	/// </remarks>
	/// <param name="phase"><see cref="PHASE_BUILD"/> / <see cref="PHASE_EVAL"/>, or <c>-1</c> if absent.</param>
	/// <param name="byteOffset">Byte offset into the msgpack expression payload, or <c>-1</c>.</param>
	/// <param name="op">Failing op name, or <c>null</c>.</param>
	/// <param name="depth">True nesting depth of the fault, or <c>-1</c>.</param>
	/// <param name="path">Op-name chain root to fault, or <c>null</c>.</param>
	/// <param name="snippet">Rendered snippet of the failing element, or <c>null</c>.</param>
	/// <param name="lang"><see cref="LANG_MSGPACK"/> / <see cref="LANG_AEL"/>, or <c>-1</c> for msgpack.</param>
	/// <param name="aelOffset">Char offset into AEL source text, or <c>-1</c>.</param>
	/// <param name="aelSpan">Byte width of the offending AEL source region, or <c>-1</c>.</param>
	public class ExpressionTrace(int phase, int byteOffset, string op, int depth, string[] path,
		string snippet, int lang, int aelOffset, int aelSpan)
	{
		//-------------------------------------------------------
		// Wire constants (mirror server proto.h).
		//-------------------------------------------------------

		/// <summary>
		/// Top-level field-45 error-detail key carrying the nested expression-trace map.
		/// </summary>
		public const int AS_ERROR_DETAIL_KEY_EXP_TRACE = 3;

		/// <summary>
		/// Nested trace key: phase (uint; <see cref="PHASE_BUILD"/> / <see cref="PHASE_EVAL"/>).
		/// </summary>
		public const int KEY_PHASE = 1;
		/// <summary>
		/// Nested trace key: byte offset into the msgpack expression payload (uint).
		/// </summary>
		public const int KEY_BYTE_OFFSET = 2;
		/// <summary>
		/// Nested trace key: failing op name (str).
		/// </summary>
		public const int KEY_OP = 3;
		/// <summary>
		/// Nested trace key: true nesting depth of the fault (uint).
		/// </summary>
		public const int KEY_DEPTH = 4;
		/// <summary>
		/// Nested trace key: op-name chain root to fault (array of str).
		/// </summary>
		public const int KEY_PATH = 5;
		/// <summary>
		/// Nested trace key: human-only rendered snippet of the failing element (str).
		/// </summary>
		public const int KEY_SNIPPET = 6;
		/// <summary>
		/// Nested trace key: eval-phase outcome (uint; <see cref="OUTCOME_FAULT"/>,
		/// <see cref="OUTCOME_FALSE"/>, or <see cref="OUTCOME_ABSENT"/>).
		/// </summary>
		public const int KEY_OUTCOME = 7;
		/// <summary>
		/// Nested trace key: source language (uint; <see cref="LANG_MSGPACK"/> / <see cref="LANG_AEL"/>).
		/// </summary>
		public const int KEY_LANG = 8;
		/// <summary>
		/// Nested trace key: char offset into AEL source text (uint).
		/// </summary>
		public const int KEY_AEL_OFFSET = 9;
		/// <summary>
		/// Nested trace key: byte width of the offending AEL source region (uint).
		/// </summary>
		public const int KEY_AEL_SPAN = 10;
		/// <summary>
		/// Nested trace key: 1-based line in AEL source (uint; reserved).
		/// </summary>
		public const int KEY_AEL_LINE = 11;
		/// <summary>
		/// Nested trace key: 1-based column in AEL source (uint; reserved).
		/// </summary>
		public const int KEY_AEL_COL = 12;
		/// <summary>
		/// Nested trace key: decisive comparison's operand values (array of two strings).
		/// </summary>
		public const int KEY_OPERANDS = 13;

		/// <summary>
		/// Phase value: expression build failed.
		/// </summary>
		public const int PHASE_BUILD = 1;
		/// <summary>
		/// Phase value: expression evaluation failed.
		/// </summary>
		public const int PHASE_EVAL = 2;

		/// <summary>
		/// Outcome value: evaluation faulted.
		/// </summary>
		public const int OUTCOME_FAULT = 1;
		/// <summary>
		/// Outcome value: the expression evaluated cleanly to false.
		/// </summary>
		public const int OUTCOME_FALSE = 2;
		/// <summary>
		/// Outcome value: a referenced bin or key was absent.
		/// </summary>
		public const int OUTCOME_ABSENT = 3;

		/// <summary>
		/// Source language: msgpack (the implied default when <c>lang</c> is absent).
		/// </summary>
		public const int LANG_MSGPACK = 1;
		/// <summary>
		/// Source language: AEL DSL.
		/// </summary>
		public const int LANG_AEL = 2;

		/// <summary>
		/// The <c>"..."</c> sentinel the server splices into <see cref="Path"/> when the
		/// true nesting depth exceeds the path-frame cap. <see cref="Depth"/> still reports
		/// the true count.
		/// </summary>
		public const string PATH_TRUNCATION_SENTINEL = "...";

		//-------------------------------------------------------
		// Fields (all optional; sentinels mark "absent").
		//-------------------------------------------------------

		private readonly int phase = phase;
		private readonly int byteOffset = byteOffset;
		private readonly string op = op;
		private readonly int depth = depth;
		private readonly string[] path = path;
		private readonly string snippet = snippet;
		private readonly int lang = lang;
		private readonly int aelOffset = aelOffset;
		private readonly int aelSpan = aelSpan;
		private readonly int outcome = -1;
		private readonly string[] operands;

		/// <summary>
		/// Construct a trace including eval-phase outcome details. Use <c>-1</c> /
		/// <c>null</c> for absent fields.
		/// </summary>
		public ExpressionTrace(int phase, int byteOffset, string op, int depth, string[] path,
			string snippet, int lang, int aelOffset, int aelSpan, int outcome, string[] operands)
			: this(phase, byteOffset, op, depth, path, snippet, lang, aelOffset, aelSpan)
		{
			this.outcome = outcome;
			this.operands = operands;
		}

		/// <summary>
		/// Phase that failed: <see cref="PHASE_BUILD"/> or <see cref="PHASE_EVAL"/>.
		/// Returns <c>-1</c> when absent.
		/// </summary>
		/// <returns>The failed phase, or <c>-1</c> when absent.</returns>
		public int Phase => phase;

		/// <summary>
		/// Byte offset into the msgpack expression payload of the failing element, or
		/// <c>-1</c> when absent. This is a coordinate into the wire payload the client
		/// sent, not into AEL source text (see <see cref="AelOffset"/>).
		/// </summary>
		public int ByteOffset => byteOffset;

		/// <summary>
		/// Failing op name (pre-rendered server-side), or <c>null</c> when absent.
		/// </summary>
		public string Op => op;

		/// <summary>
		/// True nesting depth of the fault, or <c>-1</c> when absent. Reports the true
		/// count even when <see cref="Path"/> was truncated to the frame cap.
		/// </summary>
		public int Depth => depth;

		/// <summary>
		/// Op-name chain from root to fault, or <c>null</c> when absent. May contain a
		/// <see cref="PATH_TRUNCATION_SENTINEL"/> (<c>"..."</c>) element mid-array when the true
		/// nesting exceeded the server's path-frame cap; <see cref="Depth"/> still reports
		/// the true count.
		/// </summary>
		public string[] Path => path;

		/// <summary>
		/// Human-only rendered snippet of the failing element, or <c>null</c> when absent
		/// (it is the first field the server drops under a tight byte budget).
		/// </summary>
		public string Snippet => snippet;

		/// <summary>
		/// Source language: <see cref="LANG_MSGPACK"/> or <see cref="LANG_AEL"/>. An absent
		/// <c>lang</c> key means msgpack (the default), so this returns
		/// <see cref="LANG_MSGPACK"/> when the
		/// server omitted it.
		/// </summary>
		public int Lang => (lang < 0) ? LANG_MSGPACK : lang;

		/// <summary>
		/// Char offset into the AEL source text, or <c>-1</c> when absent. A different
		/// coordinate space from <see cref="ByteOffset"/>.
		/// </summary>
		public int AelOffset => aelOffset;

		/// <summary>
		/// Byte width of the offending AEL source region, or <c>-1</c> when absent.
		/// </summary>
		public int AelSpan => aelSpan;

		/// <summary>
		/// Why an eval-phase expression did not match: <see cref="OUTCOME_FAULT"/>,
		/// <see cref="OUTCOME_FALSE"/>, or <see cref="OUTCOME_ABSENT"/>. Returns
		/// <c>-1</c> when absent. Build-phase traces do not include an outcome.
		/// </summary>
		public int Outcome => outcome;

		/// <summary>
		/// Decisive comparison values as <c>[lhs, rhs]</c>, or <c>null</c> when absent.
		/// This optional field is emitted only for a clean-false comparison and is the
		/// first field dropped to satisfy the server's error-detail byte budget. Values
		/// are server-rendered display strings capped at 48 bytes; non-scalars use
		/// placeholders such as <c>&lt;blob&gt;</c> or <c>&lt;collection&gt;</c>.
		/// </summary>
		public string[] Operands => operands;

		public override string ToString()
		{
			StringBuilder sb = new(128);
			sb.Append("ExpressionTrace[phase=").Append(phase);
			sb.Append(", byteOffset=").Append(byteOffset);
			if (op != null)
			{
				sb.Append(", op=").Append(op);
			}
			sb.Append(", depth=").Append(depth);
			if (path != null)
			{
				sb.Append(", path=").Append(string.Join(", ", path));
			}
			if (snippet != null)
			{
				sb.Append(", snippet=").Append(snippet);
			}
			// Zero is not a wire value, so this also skips a default trace.
			if (outcome > 0)
			{
				sb.Append(", outcome=").Append(outcome);
			}
			if (operands != null)
			{
				sb.Append(", operands=").Append(string.Join(", ", operands));
			}
			sb.Append(", lang=").Append(Lang);
			if (aelOffset >= 0)
			{
				sb.Append(", aelOffset=").Append(aelOffset);
			}
			if (aelSpan >= 0)
			{
				sb.Append(", aelSpan=").Append(aelSpan);
			}
			sb.Append(']');
			return sb.ToString();
		}
	}
}
