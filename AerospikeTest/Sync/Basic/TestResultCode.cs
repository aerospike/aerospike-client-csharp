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
