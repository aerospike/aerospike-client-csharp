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
	[TestClass]
	public class TestErrorDetailBatch : TestSync
	{
		private const string BinName = "edb-bin";
		private static Key listKey;

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "extended errors");
			listKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edb-list-key");
			client.Put(new WritePolicy(), listKey, new Bin(BinName, new List<int> { 10, 20, 30 }));
		}

		[TestMethod]
		public void TestBatchRowSurfacesSubcode()
		{
			BatchPolicy policy = new(client.BatchParentPolicyWriteDefault)
			{
				errorDetailVerbosity = 2
			};
			BatchRead errorRow = new(listKey, [ListOperation.Get(BinName, 99)]);
			BatchRead successRow = new(listKey, [ListOperation.Size(BinName)]);
			List<BatchRecord> records = [errorRow, successRow];

			client.Operate(policy, records);

			Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, errorRow.resultCode);
			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, errorRow.subCode);
			Assert.IsNotNull(errorRow.serverMessage);
			StringAssert.Contains(errorRow.serverMessage, "subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);

			Assert.AreEqual(ResultCode.OK, successRow.resultCode);
			Assert.IsNull(successRow.serverMessage);
			Assert.AreEqual(SubCode.NONE, successRow.subCode);
			Assert.IsNull(successRow.expTrace);
		}

		[TestMethod]
		public void TestSingleKeyBatchReadSurfacesSubcode()
		{
			BatchPolicy policy = new(client.BatchParentPolicyWriteDefault)
			{
				errorDetailVerbosity = 2
			};
			BatchRead errorRow = new(listKey, [ListOperation.Get(BinName, 99)]);
			List<BatchRecord> records = [errorRow];

			client.Operate(policy, records);

			Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, errorRow.resultCode);
			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, errorRow.subCode);
			Assert.IsNotNull(errorRow.serverMessage);
			StringAssert.Contains(errorRow.serverMessage, "subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
		}

		[TestMethod]
		public void TestSingleKeyBatchWriteSurfacesMessage()
		{
			Key intKey = new(SuiteHelpers.ns, SuiteHelpers.set, "edb-int-key");
			client.Put(new WritePolicy(), intKey, new Bin("i", 1));

			BatchPolicy policy = new(client.BatchParentPolicyWriteDefault)
			{
				errorDetailVerbosity = 2
			};
			BatchWrite errorRow = new(intKey, [Operation.Append(new Bin("i", "bad-append"))]);
			List<BatchRecord> records = [errorRow];

			client.Operate(policy, records);

			Assert.AreEqual(ResultCode.BIN_TYPE_ERROR, errorRow.resultCode);
			Assert.IsNotNull(errorRow.serverMessage);
			StringAssert.Contains(errorRow.serverMessage.ToLowerInvariant(), "append");
		}

		[TestMethod]
		public void TestBatchRowNoDetailWhenVerbosityOff()
		{
			BatchPolicy policy = new(client.BatchParentPolicyWriteDefault);
			BatchRead errorRow = new(listKey, [ListOperation.Get(BinName, 99)]);
			List<BatchRecord> records = [errorRow];

			client.Operate(policy, records);

			Assert.AreEqual(ResultCode.OP_NOT_APPLICABLE, errorRow.resultCode);
			Assert.AreEqual(SubCode.NONE, errorRow.subCode);
			Assert.IsNull(errorRow.serverMessage);
			Assert.IsNull(errorRow.expTrace);
		}
	}
}
