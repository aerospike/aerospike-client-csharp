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
using static Aerospike.Client.CommitError;

namespace Aerospike.Test
{
	/// <summary>
	/// Server-free tests for AerospikeException subclasses and message formatting.
	/// </summary>
	[TestClass]
	public class TestAerospikeException
	{
		[TestMethod]
		public void BaseExceptionFallsBackToResultString()
		{
			AerospikeException ex = new(ResultCode.KEY_NOT_FOUND_ERROR);

			Assert.AreEqual(ResultCode.KEY_NOT_FOUND_ERROR, ex.Result);
			Assert.AreEqual("Key not found", ex.BaseMessage);
			Assert.IsTrue(ex.Message.Contains("Key not found"));
		}

		[TestMethod]
		public void ParseExceptionUsesCustomMessage()
		{
			AerospikeException.Parse ex = new("bad field");

			Assert.AreEqual(ResultCode.PARSE_ERROR, ex.Result);
			Assert.AreEqual("bad field", ex.BaseMessage);
		}

		[TestMethod]
		public void SerializeExceptionWrapsInner()
		{
			Exception inner = new InvalidOperationException("boom");
			AerospikeException.Serialize ex = new(inner);

			Assert.AreEqual(ResultCode.SERIALIZE_ERROR, ex.Result);
			Assert.AreEqual("boom", ex.BaseMessage);
			Assert.AreSame(inner, ex.InnerException);
		}

		[TestMethod]
		public void ConnectionExceptionPreservesResultCode()
		{
			AerospikeException.Connection ex = new(ResultCode.NO_MORE_CONNECTIONS, "pool exhausted");

			Assert.AreEqual(ResultCode.NO_MORE_CONNECTIONS, ex.Result);
			Assert.AreEqual("pool exhausted", ex.BaseMessage);
		}

		[TestMethod]
		public void InvalidNodeExceptionDescribesPartition()
		{
			AerospikeException.InvalidNode ex = new(42);

			Assert.AreEqual(ResultCode.INVALID_NODE_ERROR, ex.Result);
			Assert.IsTrue(ex.BaseMessage.Contains("42"));
		}

		[TestMethod]
		public void InvalidNodeExceptionDescribesClusterAndPartition()
		{
			Partition partition = new("test", Replica.MASTER)
			{
				partitionId = 17
			};
			AerospikeException.InvalidNode ex = new(2, partition);

			Assert.AreEqual(ResultCode.INVALID_NODE_ERROR, ex.Result);
			Assert.IsTrue(ex.BaseMessage.Contains("partition"));
			Assert.IsTrue(ex.BaseMessage.Contains(partition.ToString()));
		}

		[TestMethod]
		public void InvalidNodeExceptionUsesCustomMessage()
		{
			AerospikeException.InvalidNode ex = new("node lookup failed");

			Assert.AreEqual(ResultCode.INVALID_NODE_ERROR, ex.Result);
			Assert.AreEqual("node lookup failed", ex.BaseMessage);
		}

		[TestMethod]
		public void InvalidNodeExceptionReportsEmptyCluster()
		{
			Partition partition = new("test", Replica.MASTER);
			AerospikeException.InvalidNode ex = new(0, partition);

			Assert.IsTrue(ex.BaseMessage.Contains("Cluster is empty"));
		}

		[TestMethod]
		public void InvalidNamespaceExceptionDescribesNamespace()
		{
			AerospikeException.InvalidNamespace ex = new("missing-ns", 3);

			Assert.AreEqual(ResultCode.INVALID_NAMESPACE, ex.Result);
			Assert.IsTrue(ex.BaseMessage.Contains("missing-ns"));
		}

		[TestMethod]
		public void TerminatedExceptionsUseResultStrings()
		{
			Assert.AreEqual(ResultCode.SCAN_TERMINATED, new AerospikeException.ScanTerminated().Result);
			Assert.AreEqual("Scan terminated", new AerospikeException.ScanTerminated().BaseMessage);
			Assert.AreEqual(ResultCode.QUERY_TERMINATED, new AerospikeException.QueryTerminated().Result);
			Assert.AreEqual("Query terminated", new AerospikeException.QueryTerminated().BaseMessage);
		}

		[TestMethod]
		public void CommandRejectedIsBackoffSubclass()
		{
			AerospikeException.CommandRejected ex = new();

			Assert.AreEqual(ResultCode.COMMAND_REJECTED, ex.Result);
			Assert.IsInstanceOfType(ex, typeof(AerospikeException.Backoff));
		}

		[TestMethod]
		public void TimeoutExceptionReportsServerStatistics()
		{
			Policy policy = new()
			{
				socketTimeout = 250,
				totalTimeout = 1000,
				maxRetries = 1
			};
			AerospikeException.Timeout ex = new(policy, client: false)
			{
				Iteration = 0,
				Policy = policy
			};

			Assert.AreEqual(ResultCode.TIMEOUT, ex.Result);
			Assert.IsFalse(ex.client);
			Assert.IsTrue(ex.Message.Contains("Server timeout:"));
			Assert.IsTrue(ex.Message.Contains("socket=250"));
			Assert.IsTrue(ex.Message.Contains("total=1000"));
		}

		[TestMethod]
		public void TimeoutExceptionReportsClientStatistics()
		{
			Policy policy = new()
			{
				socketTimeout = 1000,
				totalTimeout = 5000,
				maxRetries = 2
			};
			AerospikeException.Timeout ex = new(policy, iteration: 1);

			Assert.AreEqual(ResultCode.TIMEOUT, ex.Result);
			Assert.IsTrue(ex.Message.Contains("Client"));
			Assert.IsTrue(ex.Message.Contains("iteration=1"));
			Assert.IsTrue(ex.Message.Contains("socket=1000"));
			Assert.IsTrue(ex.Message.Contains("total=5000"));
		}

		[TestMethod]
		public void BatchExceptionsRetainPartialResults()
		{
			bool[] exists = [true, false];
			Record[] records = [new Record(null, 0, 0), null];
			BatchRecord[] batchRecords = [new BatchWrite(new Key("ns", "set", "k"), [])];

			AerospikeException.BatchExists batchExists = new(exists, new Exception("partial"));
			AerospikeException.BatchRecords batchRecordsEx = new(records, new Exception("partial"));
			AerospikeException.BatchRecordArray batchArrayEx = new(batchRecords, "failed", new Exception("partial"));

			Assert.AreSame(exists, batchExists.exists);
			Assert.AreSame(records, batchRecordsEx.records);
			Assert.AreSame(batchRecords, batchArrayEx.records);
			Assert.AreEqual(ResultCode.BATCH_FAILED, batchArrayEx.Result);
			Assert.AreEqual("failed", batchArrayEx.BaseMessage);
		}

		[TestMethod]
		public void CommitExceptionIncludesVerifyAndRollDetails()
		{
			BatchRecord failedVerify = new BatchRead(new Key("ns", "set", "k1"), false);
			failedVerify.resultCode = ResultCode.GENERATION_ERROR;
			BatchRecord[] verifyRecords = [failedVerify];
			AerospikeException.Commit ex = new(CommitErrorType.VERIFY_FAIL, verifyRecords, null);

			Assert.AreEqual(ResultCode.TXN_FAILED, ex.Result);
			Assert.AreEqual(CommitErrorType.VERIFY_FAIL, ex.Error);
			Assert.IsTrue(ex.Message.Contains("verify errors:"));
			Assert.IsTrue(ex.Message.Contains("k1"));
		}
	}
}
