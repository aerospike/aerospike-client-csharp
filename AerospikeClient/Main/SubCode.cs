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
	/// Server error detail subcodes.
	/// </summary>
	/// <remarks>
	/// <para>
	/// When extended error detail is requested with <see cref="Policy.errorDetailVerbosity"/>,
	/// the server may attach a numeric subcode to a failure response. The subcode is surfaced
	/// on <see cref="AerospikeException.SubCode"/>.
	/// </para>
	/// <para>
	/// Match on the <c>(resultCode, subCode)</c> pair. Subcode integer values are scoped to
	/// their parent <see cref="ResultCode"/> and are not globally unique. For example, the
	/// value <c>1</c> recurs under multiple parent status codes.
	/// </para>
	/// <code>
	/// catch (AerospikeException ae)
	/// {
	/// 	if (ae.Result == ResultCode.OP_NOT_APPLICABLE &amp;&amp;
	/// 		ae.SubCode == SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW)
	/// 	{
	/// 		// Handle bounded-list overflow.
	/// 	}
	/// }
	/// </code>
	/// <para>
	/// <see cref="NONE"/> means no subcode. It is returned when the server did not send a
	/// subcode because verbosity was disabled or the failing branch had no dispatchable subcode.
	/// </para>
	/// <para>
	/// This catalog mirrors the server's per-status enums in <c>as/include/base/proto.h</c>.
	/// Published values are immutable and are not renumbered or reused. Treat undeclared
	/// subcode values as opaque integers.
	/// </para>
	/// </remarks>
	public static class SubCode
	{
		/// <summary>
		/// No subcode (universal). Returned when the server did not supply a subcode.
		/// Value: 0
		/// </summary>
		public const int NONE = 0;

		//-------------------------------------------------------
		// Pairs with ResultCode.PARAMETER_ERROR (4)  [AS_ERR_PARAMETER]
		//-------------------------------------------------------

		/// <summary>
		/// Per-record TTL exceeds the namespace's max-ttl.
		/// Value: 1
		/// </summary>
		public const int PARAM_TTL_INVALID = 1;

		/// <summary>
		/// Bit op offset lands past the blob (or above the proto cap).
		/// Value: 2
		/// </summary>
		public const int PARAM_BITS_OFFSET_OUT_OF_RANGE = 2;

		/// <summary>
		/// Bit op size is out of range (e.g. zero, or too large).
		/// Value: 3
		/// </summary>
		public const int PARAM_BITS_SIZE_OUT_OF_RANGE = 3;

		/// <summary>
		/// Blob resize would exceed the maximum blob size.
		/// Value: 4
		/// </summary>
		public const int PARAM_BITS_RESIZE_EXCEEDED = 4;

		/// <summary>
		/// Write would exceed the per-record bin-count limit (write path).
		/// Value: 5
		/// </summary>
		public const int PARAM_BIN_COUNT_TOO_LARGE = 5;

		/// <summary>
		/// String op wire/expression args malformed or out of range.
		/// Value: 6
		/// </summary>
		public const int PARAM_STRING_OP_PARAMS_INVALID = 6;

		/// <summary>
		/// String op code or modifier/read class mismatch on the wire path.
		/// Value: 7
		/// </summary>
		public const int PARAM_STRING_OP_INVALID = 7;

		/// <summary>
		/// String context-eval envelope malformed. Server constant
		/// <c>AS_SUB_PARAM_STRING_CTX_MALFORMED</c>.
		/// Value: 8
		/// </summary>
		public const int PARAM_STRING_CTX_MALFORMED = 8;

		/// <summary>
		/// String modify/read index or code-point range out of bounds.
		/// Value: 9
		/// </summary>
		public const int PARAM_STRING_INDEX_OUT_OF_BOUNDS = 9;

		/// <summary>
		/// String regex pattern invalid (compile / ICU failure).
		/// Value: 10
		/// </summary>
		public const int PARAM_STRING_REGEX_INVALID = 10;

		/// <summary>
		/// String or string op argument is not valid UTF-8.
		/// Value: 11
		/// </summary>
		public const int PARAM_STRING_UTF8_INVALID = 11;

		//-------------------------------------------------------
		// Pairs with ResultCode.PARTITION_UNAVAILABLE (11)  [AS_ERR_UNAVAILABLE]
		//-------------------------------------------------------

		/// <summary>
		/// Cluster is still resolving initial partition balance at startup.
		/// Value: 1
		/// </summary>
		public const int UNAVAIL_INITIAL_BALANCE_UNRESOLVED = 1;

		/// <summary>
		/// A needed replica is unavailable (likely a partition split).
		/// Value: 2
		/// </summary>
		public const int UNAVAIL_REPLICA_UNAVAILABLE = 2;

		//-------------------------------------------------------
		// Pairs with ResultCode.UNSUPPORTED_FEATURE (16)  [AS_ERR_UNSUPPORTED_FEATURE]
		//-------------------------------------------------------

		/// <summary>
		/// MRT attempted against a non-SC (AP) namespace.
		/// Value: 1
		/// </summary>
		public const int UNSUPP_FEAT_MRT_REQUIRES_STRONG_CONSISTENCY = 1;

		/// <summary>
		/// Requested feature is unsupported in this context (generic).
		/// Value: 2
		/// </summary>
		public const int UNSUPP_FEAT_GENERIC = 2;

		//-------------------------------------------------------
		// Pairs with ResultCode.BIN_NOT_FOUND (17)  [AS_ERR_BIN_NOT_FOUND]
		//-------------------------------------------------------

		/// <summary>
		/// HLL op needs an existing bin and can't auto-create one.
		/// Value: 1
		/// </summary>
		public const int BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP = 1;

		// Server subcode 2 in this family was dropped as unreachable: a string modify on a
	    // missing bin returns AS_OK with the bin uncreated, never BIN_NOT_FOUND.

		//-------------------------------------------------------
		// Pairs with ResultCode.BIN_NAME_TOO_LONG (21)  [AS_ERR_BIN_NAME]
		//-------------------------------------------------------

		/// <summary>
		/// Write would exceed the per-record bin-count limit (UDF path).
		/// Value: 1
		/// </summary>
		public const int BIN_NAME_COUNT_TOO_LARGE = 1;

		//-------------------------------------------------------
		// Pairs with ResultCode.FAIL_FORBIDDEN (22)  [AS_ERR_FORBIDDEN]
		//-------------------------------------------------------

		/// <summary>
		/// Write bounced by an XDR ship filter at the destination.
		/// Value: 1
		/// </summary>
		public const int FORBID_XDR_FILTER_BLOCKED = 1;

		/// <summary>
		/// Set-level record-count stop-writes limit reached.
		/// Value: 2
		/// </summary>
		public const int FORBID_SET_COUNT_STOP_WRITES = 2;

		/// <summary>
		/// Set-level size stop-writes limit reached.
		/// Value: 3
		/// </summary>
		public const int FORBID_SET_SIZE_STOP_WRITES = 3;

		/// <summary>
		/// Writes stopped due to cluster clock skew.
		/// Value: 4
		/// </summary>
		public const int FORBID_CLOCK_SKEW_STOP_WRITES = 4;

		/// <summary>
		/// REPLACE / CREATE_OR_REPLACE forbidden while resolving conflicts.
		/// Value: 5
		/// </summary>
		public const int FORBID_REPLACE_CONFLICT_RESOLVING = 5;

		/// <summary>
		/// Write forbidden because the set/namespace is mid-truncate.
		/// Value: 6
		/// </summary>
		public const int FORBID_TRUNCATED = 6;

		// Note: server subcodes 7 and 9 in this family are retired (masking violations
		// return ROLE_VIOLATION, not FORBIDDEN) and are intentionally not declared.

		/// <summary>
		/// Non-durable delete forbidden (would violate durability).
		/// Value: 8
		/// </summary>
		public const int FORBID_DURABILITY_VIOLATION = 8;

		//-------------------------------------------------------
		// Pairs with ResultCode.OP_NOT_APPLICABLE (26)  [AS_ERR_OP_NOT_APPLICABLE]
		//-------------------------------------------------------

		/// <summary>
		/// List index is outside the current element range.
		/// Value: 1
		/// </summary>
		public const int OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1;

		/// <summary>
		/// Requested rank is past the current population.
		/// Value: 2
		/// </summary>
		public const int OPNOT_CDT_RANK_OUT_OF_BOUNDS = 2;

		/// <summary>
		/// Insert would exceed an ordered+bounded list's cap.
		/// Value: 3
		/// </summary>
		public const int OPNOT_CDT_BOUNDED_LIST_OVERFLOW = 3;

		/// <summary>
		/// HLL op needs index_bits but the sketch has none set.
		/// Value: 4
		/// </summary>
		public const int OPNOT_HLL_INDEX_BITS_UNSET = 4;

		/// <summary>
		/// Union needs to reduce index_bits but folding isn't allowed.
		/// Value: 5
		/// </summary>
		public const int OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS = 5;

		/// <summary>
		/// As above, for the minhash dimension.
		/// Value: 6
		/// </summary>
		public const int OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS = 6;

		/// <summary>
		/// Fold blocked because the sketch carries minhash bits.
		/// Value: 7
		/// </summary>
		public const int OPNOT_HLL_CANNOT_FOLD_MINHASH = 7;

		/// <summary>
		/// Fold target index_bits >= current (fold can only reduce).
		/// Value: 8
		/// </summary>
		public const int OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE = 8;

		/// <summary>
		/// Intersect inputs have mismatched minhash parameters.
		/// Value: 9
		/// </summary>
		public const int OPNOT_HLL_INTERSECT_MINHASH_MISMATCH = 9;

		/// <summary>
		/// String to numeric conversion failed (strtoll/strtod).
		/// Value: 10
		/// </summary>
		public const int OPNOT_STRING_CONVERSION_FAILED = 10;

		/// <summary>
		/// Source blob/string is not valid UTF-8 for an OP_NOT_APPLICABLE path.
		/// Value: 11
		/// </summary>
		public const int OPNOT_STRING_UTF8_INVALID = 11;

		/// <summary>
		/// ICU regex resource limit exceeded.
		/// Value: 12
		/// </summary>
		public const int OPNOT_STRING_REGEX_LIMIT_EXCEEDED = 12;

		/// <summary>
		/// String is not valid base64 — a length that is not a multiple of 4, a character
		/// outside the alphabet, or misplaced <c>'='</c> padding.
		/// Value: 13
		/// </summary>
		public const int OPNOT_STRING_B64_INVALID = 13;

		//-------------------------------------------------------
		// ResultCode.FILTERED_OUT (27) [AS_ERR_FILTERED_OUT] carries NO subcode:
		// the server emits AS_SUB_NONE plus a contextual "filtered out ..." message.
		// (The as_sub_filtered_t enum was removed server-side and never shipped, so
		// no FILTERED_* constants are defined here. Match on the message, not a subcode.)
		//-------------------------------------------------------

		//-------------------------------------------------------
		// Pairs with ResultCode.MRT_BLOCKED (120)  [AS_ERR_MRT_BLOCKED]
		//-------------------------------------------------------

		/// <summary>
		/// Record is provisionally locked by another MRT.
		/// Value: 1
		/// </summary>
		public const int MRT_BLOCKED_RECORD_LOCKED = 1;

		/// <summary>
		/// Op belongs to a different MRT than the one holding the lock.
		/// Value: 2
		/// </summary>
		public const int MRT_BLOCKED_ID_MISMATCH = 2;
	}
}
