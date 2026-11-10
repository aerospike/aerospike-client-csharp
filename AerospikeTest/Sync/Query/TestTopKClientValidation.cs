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
	/// Client-side Top-K request validation that runs before anything is sent (R28),
	/// aligned with the Node.js Top-K plan cases TK-K1, TK-R*, and TK-S*.
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

		/// <summary>TK-K1 / TK-R3 / TK-R4: reject huge or zero k before allocating.</summary>
		[TestMethod]
		public void RejectsKOutsideOneToThousand()
		{
			Statement stmt = new();
			foreach (int k in new int[] { 0, 1001, 1 << 20, int.MaxValue, -1 })
			{
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => stmt.SetTopK(k),
					"expected SetTopK(" + k + ") to throw");
				Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
				Assert.AreEqual("topK must be in [1, 1000]; got " + k, ae.BaseMessage);
			}
		}

		/// <summary>TK-R5: k without an order key.</summary>
		[TestMethod]
		public void RejectsTopKWithoutOrderBy()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("Top-K requires an order key; call SetOrderBy()", ae.BaseMessage);
		}

		/// <summary>TK-S3: thenOrderBy before orderBy.</summary>
		[TestMethod]
		public void RejectsThenOrderByBeforeOrderBy()
		{
			Statement stmt = new();
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("thenOrderBy must follow orderBy", ae.BaseMessage);
		}

		/// <summary>TK-R7: at most two order keys.</summary>
		[TestMethod]
		public void RejectsThirdOrderKey()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => stmt.ThenOrderBy("extra", BinDataType.INTEGER, Order.ASC));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("orderBy supports at most two keys", ae.BaseMessage);
		}

		/// <summary>TK-S3b / TK-S4: a later orderBy replaces keys; a later topK replaces k.</summary>
		[TestMethod]
		public void OrderByReplacementClearsThenKeepsLatestTopK()
		{
			Statement stmt = new();
			stmt.SetOrderBy("score", BinDataType.INTEGER, Order.DESC);
			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);
			stmt.SetTopK(5);

			Assert.IsTrue(stmt.HasTopKThen);
			Assert.AreEqual(5, stmt.TopKLimit);

			stmt.SetOrderBy("price", BinDataType.INTEGER, Order.ASC);
			Assert.IsFalse(stmt.HasTopKThen);
			Assert.AreEqual("price", stmt.TopKBin);
			Assert.AreEqual(5, stmt.TopKLimit);

			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);
			stmt.SetTopK(7);
			Assert.IsTrue(stmt.HasTopKThen);
			Assert.AreEqual("price", stmt.TopKBin);
			Assert.AreEqual("ts", stmt.TopKThenBin);
			Assert.AreEqual(7, stmt.TopKLimit);
		}

		/// <summary>TK-R6: unsupported order type (STRING is not in BinDataType for this release).</summary>
		[TestMethod]
		public void RejectsUnsupportedOrderByType()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, (BinDataType)5, Order.DESC);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("orderBy type must be INTEGER, DOUBLE, or BYTES; got 5", ae.BaseMessage);
		}

		/// <summary>TK-R6: empty / oversized / NUL bin names.</summary>
		[TestMethod]
		public void RejectsInvalidOrderByBinName()
		{
			foreach (string bin in new string[] { "", "abcdefghijklmnop", "sc\0re" })
			{
				Statement stmt = new();
				stmt.SetOrderBy(bin, BinDataType.INTEGER, Order.DESC);
				stmt.SetTopK(3);

				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => Internals.ValidateTopK(stmt),
					"expected invalid bin '" + bin.Replace("\0", "\\0") + "' to throw");
				Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
				Assert.AreEqual(
					"Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL",
					ae.BaseMessage);
			}
		}

		/// <summary>TK-R13: same bin as both order keys.</summary>
		[TestMethod]
		public void RejectsDuplicateOrderByBins()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.ThenOrderBy(Bin, BinDataType.INTEGER, Order.ASC);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("orderBy keys must use different bins", ae.BaseMessage);
		}

		/// <summary>TK-R9: order key missing from projection.</summary>
		[TestMethod]
		public void RejectsOrderKeyNotInProjection()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetBinNames("other");
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual(
				"orderBy bin '" + Bin + "' is not in projection; add it to the projection or remove the projection",
				ae.BaseMessage);
		}

		/// <summary>TK-R9: two ops producing the order key.</summary>
		[TestMethod]
		public void RejectsOrderKeyProducedByMultipleOps()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.Operations =
			[
				Operation.Get(Bin),
				Operation.Get(Bin)
			];
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual(
				"orderBy bin '" + Bin + "' is produced by more than one operation",
				ae.BaseMessage);
		}

		/// <summary>TK-R1: Top-K incompatible with aggregate UDFs.</summary>
		[TestMethod]
		public void RejectsAggregateUdf()
		{
			Statement stmt = TopKStatement();
			stmt.SetAggregateFunction("pkg", "fn");

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual(
				"orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage",
				ae.BaseMessage);
		}

		/// <summary>TK-R2: Top-K on a background execute is rejected before send.</summary>
		[TestMethod]
		public void RejectsBackgroundExecuteTopK()
		{
			RequireCapableCluster();

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.Execute(null, TopKStatement(), "pkg", "fn"));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("synchronous"));
		}

		/// <summary>
		/// TK-R10: statement maxRecords conflicts with Top-K (policy maxRecords covered above).
		/// </summary>
		[TestMethod]
		public void RejectsStatementMaxRecords()
		{
			Statement stmt = TopKStatement();
#pragma warning disable 0618
			stmt.maxRecords = 10;
#pragma warning restore 0618

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("Top-K is incompatible with maxRecords", ae.BaseMessage);
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
