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
	/// Server-free tests for <see cref="ResultCode.GetResultString"/> and
	/// <see cref="ResultCode.KeepConnection"/>.
	/// </summary>
	[TestClass]
	public class TestResultCode
	{
		private static readonly (int Code, string Message)[] KnownResultStrings =
		[
			(ResultCode.TXN_ALREADY_ABORTED, "Transaction already aborted"),
			(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed"),
			(ResultCode.TXN_FAILED, "Transaction failed"),
			(ResultCode.BATCH_FAILED, "One or more keys failed in a batch"),
			(ResultCode.NO_RESPONSE, "No response received from server"),
			(ResultCode.MAX_ERROR_RATE, "Max error rate exceeded"),
			(ResultCode.MAX_RETRIES_EXCEEDED, "Max retries exceeded"),
			(ResultCode.SERIALIZE_ERROR, "Serialize error"),
			(ResultCode.SERVER_NOT_AVAILABLE, "Server not available"),
			(ResultCode.NO_MORE_CONNECTIONS, "No more available connections"),
			(ResultCode.COMMAND_REJECTED, "Command rejected"),
			(ResultCode.QUERY_TERMINATED, "Query terminated"),
			(ResultCode.SCAN_TERMINATED, "Scan terminated"),
			(ResultCode.INVALID_NODE_ERROR, "Invalid node"),
			(ResultCode.PARSE_ERROR, "Parse error"),
			(ResultCode.CLIENT_ERROR, "Client error"),
			(ResultCode.OK, "ok"),
			(ResultCode.SERVER_ERROR, "Server error"),
			(ResultCode.KEY_NOT_FOUND_ERROR, "Key not found"),
			(ResultCode.GENERATION_ERROR, "Generation error"),
			(ResultCode.PARAMETER_ERROR, "Parameter error"),
			(ResultCode.KEY_EXISTS_ERROR, "Key already exists"),
			(ResultCode.BIN_EXISTS_ERROR, "Bin already exists"),
			(ResultCode.CLUSTER_KEY_MISMATCH, "Cluster key mismatch"),
			(ResultCode.SERVER_MEM_ERROR, "Server memory error"),
			(ResultCode.TIMEOUT, "Timeout"),
			(ResultCode.ALWAYS_FORBIDDEN, "Operation not allowed"),
			(ResultCode.PARTITION_UNAVAILABLE, "Partition unavailable"),
			(ResultCode.BIN_TYPE_ERROR, "Bin type error"),
			(ResultCode.RECORD_TOO_BIG, "Record too big"),
			(ResultCode.KEY_BUSY, "Hot key"),
			(ResultCode.SCAN_ABORT, "Scan aborted"),
			(ResultCode.UNSUPPORTED_FEATURE, "Unsupported server feature"),
			(ResultCode.BIN_NOT_FOUND, "Bin not found"),
			(ResultCode.DEVICE_OVERLOAD, "Device overload"),
			(ResultCode.KEY_MISMATCH, "Key mismatch"),
			(ResultCode.INVALID_NAMESPACE, "Namespace not found"),
			(ResultCode.BIN_NAME_TOO_LONG, "Bin name length greater than 15 characters or maximum bins exceeded"),
			(ResultCode.FAIL_FORBIDDEN, "Operation not allowed at this time"),
			(ResultCode.ELEMENT_NOT_FOUND, "Map key not found"),
			(ResultCode.ELEMENT_EXISTS, "Map key exists"),
			(ResultCode.ENTERPRISE_ONLY, "Enterprise only"),
			(ResultCode.OP_NOT_APPLICABLE, "Operation not applicable"),
			(ResultCode.FILTERED_OUT, "Command filtered out"),
			(ResultCode.LOST_CONFLICT, "Command failed due to conflict with XDR"),
			(ResultCode.INVALID_ENCODING, "Invalid UTF-8 encoding"),
			(ResultCode.XDR_KEY_BUSY, "Write can't complete until XDR finishes shipping."),
			(ResultCode.QUERY_END, "Query end"),
			(ResultCode.SECURITY_NOT_SUPPORTED, "Security not supported"),
			(ResultCode.SECURITY_NOT_ENABLED, "Security not enabled"),
			(ResultCode.SECURITY_SCHEME_NOT_SUPPORTED, "Security scheme not supported"),
			(ResultCode.INVALID_COMMAND, "Invalid command"),
			(ResultCode.INVALID_FIELD, "Invalid field"),
			(ResultCode.ILLEGAL_STATE, "Illegal State"),
			(ResultCode.INVALID_USER, "Invalid user"),
			(ResultCode.USER_ALREADY_EXISTS, "User already exists"),
			(ResultCode.INVALID_PASSWORD, "Invalid password"),
			(ResultCode.EXPIRED_PASSWORD, "Password expired"),
			(ResultCode.FORBIDDEN_PASSWORD, "Password can't be reused"),
			(ResultCode.INVALID_CREDENTIAL, "Invalid credential"),
			(ResultCode.EXPIRED_SESSION, "Login session expired"),
			(ResultCode.INVALID_ROLE, "Invalid role"),
			(ResultCode.ROLE_ALREADY_EXISTS, "Role already exists"),
			(ResultCode.INVALID_PRIVILEGE, "Invalid privilege"),
			(ResultCode.INVALID_WHITELIST, "Invalid whitelist"),
			(ResultCode.QUOTAS_NOT_ENABLED, "Quotas not enabled"),
			(ResultCode.INVALID_QUOTA, "Invalid quota"),
			(ResultCode.NOT_AUTHENTICATED, "Not authenticated"),
			(ResultCode.ROLE_VIOLATION, "Role violation"),
			(ResultCode.NOT_WHITELISTED, "Command not whitelisted"),
			(ResultCode.QUOTA_EXCEEDED, "Quota exceeded"),
			(ResultCode.UDF_BAD_RESPONSE, "UDF returned error"),
			(ResultCode.MRT_BLOCKED, "Transaction record blocked by a different transaction"),
			(ResultCode.MRT_VERSION_MISMATCH, "Transaction version mismatch"),
			(ResultCode.MRT_EXPIRED, "Transaction expired"),
			(ResultCode.MRT_TOO_MANY_WRITES, "Transaction write command limit exceeded"),
			(ResultCode.MRT_COMMITTED, "Transaction already committed"),
			(ResultCode.MRT_ABORTED, "Transaction already aborted"),
			(ResultCode.MRT_ALREADY_LOCKED, "This record has been locked by a previous update in this transaction"),
			(ResultCode.MRT_MONITOR_EXISTS, "This transaction has already started. Writing to the same transaction with independent threads is unsafe"),
			(ResultCode.BATCH_DISABLED, "Batch functionality has been disabled"),
			(ResultCode.BATCH_MAX_REQUESTS_EXCEEDED, "Batch max requests have been exceeded"),
			(ResultCode.BATCH_QUEUES_FULL, "All batch queues are full"),
			(ResultCode.INDEX_ALREADY_EXISTS, "Index already exists"),
			(ResultCode.INDEX_NOTFOUND, "Index not found"),
			(ResultCode.INDEX_OOM, "Index out of memory"),
			(ResultCode.INDEX_NOTREADABLE, "Index not readable"),
			(ResultCode.INDEX_GENERIC, "Index error"),
			(ResultCode.INDEX_NAME_MAXLEN, "Index name max length exceeded"),
			(ResultCode.INDEX_MAXCOUNT, "Index count exceeds max"),
			(ResultCode.QUERY_ABORTED, "Query aborted"),
			(ResultCode.QUERY_QUEUEFULL, "Query queue full"),
			(ResultCode.QUERY_TIMEOUT, "Query timeout"),
			(ResultCode.QUERY_GENERIC, "Query error"),
		];

		[TestMethod]
		public void GetResultStringReturnsKnownMessages()
		{
			Assert.AreEqual("ok", ResultCode.GetResultString(ResultCode.OK));
			Assert.AreEqual("Key not found", ResultCode.GetResultString(ResultCode.KEY_NOT_FOUND_ERROR));
			Assert.AreEqual("Timeout", ResultCode.GetResultString(ResultCode.TIMEOUT));
			Assert.AreEqual("Bin type error", ResultCode.GetResultString(ResultCode.BIN_TYPE_ERROR));
			Assert.AreEqual("One or more keys failed in a batch", ResultCode.GetResultString(ResultCode.BATCH_FAILED));
			Assert.AreEqual("Transaction failed", ResultCode.GetResultString(ResultCode.TXN_FAILED));
			Assert.AreEqual("Security not enabled", ResultCode.GetResultString(ResultCode.SECURITY_NOT_ENABLED));
			Assert.AreEqual("Not authenticated", ResultCode.GetResultString(ResultCode.NOT_AUTHENTICATED));
			Assert.AreEqual("Role violation", ResultCode.GetResultString(ResultCode.ROLE_VIOLATION));
			Assert.AreEqual("Query aborted", ResultCode.GetResultString(ResultCode.QUERY_ABORTED));
			Assert.AreEqual("Index not found", ResultCode.GetResultString(ResultCode.INDEX_NOTFOUND));
		}

		[TestMethod]
		public void GetResultStringCoversAllMappedCodes()
		{
			foreach ((int code, string message) in KnownResultStrings)
			{
				Assert.AreEqual(message, ResultCode.GetResultString(code), $"Result code {code}");
			}
		}

		[TestMethod]
		public void GetResultStringReturnsEmptyForUnknownCode()
		{
			Assert.AreEqual(string.Empty, ResultCode.GetResultString(99999));
			Assert.AreEqual(string.Empty, ResultCode.GetResultString(-999));
		}

		[TestMethod]
		public void KeepConnectionHonorsClientAndAbortCodes()
		{
			Assert.IsFalse(ResultCode.KeepConnection(ResultCode.CLIENT_ERROR));
			Assert.IsFalse(ResultCode.KeepConnection(ResultCode.SCAN_ABORT));
			Assert.IsFalse(ResultCode.KeepConnection(ResultCode.QUERY_ABORTED));
			Assert.IsTrue(ResultCode.KeepConnection(ResultCode.TIMEOUT));
			Assert.IsTrue(ResultCode.KeepConnection(ResultCode.KEY_NOT_FOUND_ERROR));
		}
	}
}
