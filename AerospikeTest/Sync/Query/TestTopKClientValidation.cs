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
	/// Client-side Top-K request validation that runs before anything is sent (R28).
	/// Live-cluster tests require every node to advertise query-order-by.
	/// </summary>
	[TestClass]
	public class TestTopKClientValidation : TestSync
	{
		private const string Set = "topKClientVal";
		private const string Bin = "rank";

		private static void RequireCapableCluster()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_2_1, "Top-K");

			Node[] nodes = client.Nodes;
			if (nodes.Length == 0)
			{
				Assert.Inconclusive("No cluster nodes");
			}

			foreach (Node node in nodes)
			{
				if (!node.HasQueryOrderBy)
				{
					Assert.Inconclusive("Cluster must advertise query-order-by");
				}
			}
		}

		private static Statement TopKStatement()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetBinNames(Bin);
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);
			return stmt;
		}

		[TestMethod]
		public void RejectsIncludeBinDataFalse()
		{
			RequireCapableCluster();

			QueryPolicy policy = new() { includeBinData = false };
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.Query(policy, TopKStatement()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("includeBinData"));
		}

		[TestMethod]
		public void RejectsPolicyMaxRecords()
		{
			RequireCapableCluster();

#pragma warning disable 0618
			QueryPolicy policy = new() { maxRecords = 10 };
#pragma warning restore 0618
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.Query(policy, TopKStatement()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("maxRecords"));
		}

		[TestMethod]
		public void RejectsResumedPartitionFilter()
		{
			RequireCapableCluster();

			Key after = new(SuiteHelpers.ns, Set, "resume-cursor");
			PartitionFilter filter = PartitionFilter.After(after);

			Assert.IsTrue(AerospikeClient.PartitionFilterHasResumePoints(filter));

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.QueryPartitions(null, TopKStatement(), filter));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("resume"));
		}

		[TestMethod]
		public void RejectsListenerQueryTopK()
		{
			RequireCapableCluster();

			QueryListener listener = (key, record) => { };
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.Query(null, TopKStatement(), listener));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("synchronous"));
		}

		[TestMethod]
		public void RejectsAsyncTopK()
		{
			RequireCapableCluster();

			IAsyncClient asyncClient = SuiteHelpers.asyncClient;
			if (asyncClient == null)
			{
				Assert.Inconclusive("No async client");
			}

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => asyncClient.Query(null, new NullRecordSequenceListener(), TopKStatement()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("synchronous"));
		}

		[TestMethod]
		public void PartitionFilterWithoutResumePointsIsClean()
		{
			Assert.IsFalse(AerospikeClient.PartitionFilterHasResumePoints(PartitionFilter.All()));
			Assert.IsFalse(AerospikeClient.PartitionFilterHasResumePoints(PartitionFilter.Id(0)));
		}

		[TestMethod]
		public void TopKDoesNotRetryOnTimeout()
		{
			Statement topK = TopKStatement();
			PartitionTracker topKTracker = new(new QueryPolicy(), topK, (Node[])null);
			Assert.IsFalse(topKTracker.RetryOnTimeout);
			Assert.IsFalse(topKTracker.ShouldRetry(null, new AerospikeException(ResultCode.TIMEOUT)));

			Statement plain = new();
			plain.SetNamespace("test");
			plain.SetSetName(Set);
			PartitionTracker plainTracker = new(new QueryPolicy(), plain, (Node[])null);
			Assert.IsTrue(plainTracker.RetryOnTimeout);
		}

		private sealed class NullRecordSequenceListener : RecordSequenceListener
		{
			public void OnRecord(Key key, Record record)
			{
			}

			public void OnSuccess()
			{
			}

			public void OnFailure(AerospikeException exception)
			{
			}
		}
	}

	/// <summary>
	/// Wire-level LUT field parsing (FieldType 14) used by Top-K merge (R29).
	/// </summary>
	[TestClass]
	public class TestLutFieldParsing
	{
		[TestMethod]
		public void ParseLutFieldReadsBigEndianInt64()
		{
			byte[] buf = new byte[8];
			ByteUtil.LongToBytes(0x0123456789ABCDEFUL, buf, 0);

			Assert.AreEqual(unchecked((long)0x0123456789ABCDEFUL), Command.ParseLutField(buf, 0, 8));
		}

		[TestMethod]
		public void ParseLutFieldRejectsWrongSize()
		{
			byte[] buf = new byte[4];
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Command.ParseLutField(buf, 0, 4));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("8 bytes"));
		}
	}
}
