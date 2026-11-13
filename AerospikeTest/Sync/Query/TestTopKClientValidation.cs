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
	/// Client-side Top-K request validation that runs before anything is sent.
	/// Live-cluster tests require every node to advertise query-order-by.
	/// </summary>
	[TestClass]
	public class TestTopKClientValidation : TestSync
	{
		private const string Set = "topKClientVal";
		private const string Bin = "rank";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			// Statement-only validation needs no feature bit; live methods call RequireCapableCluster.
		}

		private static void RequireCapableCluster() => RequireQueryOrderBySupport();

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

		/// <summary>
		/// Top-K skips partition-round TIMEOUT retries. Per-node query commands also
		/// use SyncCommand maxRetries 0 (scan/query ctor), so a socket/server TIMEOUT
		/// fails that node attempt immediately; <see cref="QueryPolicy.maxRetries"/>
		/// only governs PartitionTracker rounds. The partition executor only emits the
		/// merged Top-K buffer after the cluster round completes, so a TIMEOUT yields
		/// the error with no partial Top-K records.
		/// </summary>
		[TestMethod]
		public void TopKDoesNotRetryOnTimeout()
		{
			Statement topK = TopKStatement();
			PartitionTracker topKTracker = new(new QueryPolicy(), topK, (Node[])null);
			Assert.IsFalse(topKTracker.RetryOnTimeout);
			Assert.IsFalse(topKTracker.ShouldRetry(null, new AerospikeException(ResultCode.TIMEOUT)));
			Assert.IsFalse(topKTracker.ShouldRetry(null, new AerospikeException(ResultCode.DEVICE_OVERLOAD)));
			Assert.IsFalse(topKTracker.ShouldRetry(null, new AerospikeException(ResultCode.QUERY_ABORTED)));

			Statement plain = new();
			plain.SetNamespace("test");
			plain.SetSetName(Set);
			PartitionTracker plainTracker = new(new QueryPolicy(), plain, (Node[])null);
			Assert.IsTrue(plainTracker.RetryOnTimeout);
		}

		/// <summary>Reject huge or zero k before allocating.</summary>
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

		/// <summary>Reject k without an order key.</summary>
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

		/// <summary>Reject thenOrderBy before orderBy.</summary>
		[TestMethod]
		public void RejectsThenOrderByBeforeOrderBy()
		{
			Statement stmt = new();
			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ae.Result);
			Assert.AreEqual("thenOrderBy must follow orderBy", ae.BaseMessage);
		}

		/// <summary>Reject a third order key.</summary>
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

		/// <summary>A later orderBy replaces keys; a later topK replaces k.</summary>
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

		/// <summary>Reject unsupported order type.</summary>
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

		/// <summary>Reject empty, oversized, or NUL bin names.</summary>
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

		/// <summary>Reject the same bin as both order keys.</summary>
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

		/// <summary>Reject an order key missing from the projection.</summary>
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

		/// <summary>Reject an order key produced by more than one op.</summary>
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

		/// <summary>Reject Top-K combined with an aggregate UDF.</summary>
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

		/// <summary>Reject Top-K on a background execute before send.</summary>
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
		/// Statement maxRecords conflicts with Top-K (policy maxRecords covered above).
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

		/// <summary>AE-05: more than 65,535 ops is rejected before send (uint16 wire count).</summary>
		[TestMethod]
		public void RejectsTooManyOperations()
		{
			Operation[] ops = new Operation[ushort.MaxValue + 1];
			ops[0] = Operation.Get(Bin);
			for (int i = 1; i < ops.Length; i++)
			{
				ops[i] = Operation.Get("x");
			}

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.Operations = ops;
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(stmt));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
			Assert.IsTrue(ae.Message.Contains("65535"));
		}

		/// <summary>Exp.Val(Vector) and Exp.VectorBin mark hasVector for cluster guards (AG / AW).</summary>
		[TestMethod]
		public void ExpValVectorHasVector()
		{
			Vector v = Vector.OfFloat32([1f, 2f, 3f]);
			Assert.IsTrue(Internals.HasVector(Exp.Build(Exp.Val(v))));
			Assert.IsTrue(Internals.HasVector(Exp.Build(Exp.VectorBin("v"))));
		}

		/// <summary>AT-07: exactly 15-byte ASCII/UTF-8 order bins are accepted; longer/empty/NUL refused.</summary>
		[TestMethod]
		public void AcceptsFifteenByteOrderBinNames()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			stmt.SetBinNames("abcdefghijklmno"); // 15 ASCII
			stmt.SetOrderBy("abcdefghijklmno", BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(1);
			Internals.ValidateTopK(stmt);

			stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(Set);
			// 5 Cyrillic letters = 10 UTF-8 bytes; pad with ASCII to 15 bytes total.
			string utf8Bin = "абвгд12345"; // 5*2 + 5 = 15 bytes
			Assert.AreEqual(15, System.Text.Encoding.UTF8.GetByteCount(utf8Bin));
			stmt.SetBinNames(utf8Bin);
			stmt.SetOrderBy(utf8Bin, BinDataType.INTEGER, Order.ASC);
			stmt.SetTopK(2);
			Internals.ValidateTopK(stmt);
		}

		/// <summary>AE-01: representative server/client Top-K rejections surface PARAMETER/PARSE errors.</summary>
		[TestMethod]
		public void TopKRejectionCatalog()
		{
			RequireCapableCluster();

			Statement orderOnly = new();
			orderOnly.SetNamespace(SuiteHelpers.ns);
			orderOnly.SetSetName(Set);
			orderOnly.SetBinNames(Bin);
			orderOnly.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			AerospikeException orderWithoutK = Assert.Throws<AerospikeException>(
				() => { using RecordSet rs = client.Query(null, orderOnly); while (rs.Next()) { } });
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, orderWithoutK.Result);

			QueryPolicy noBins = new() { includeBinData = false };
			AerospikeException include = Assert.Throws<AerospikeException>(
				() => client.Query(noBins, TopKStatement()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, include.Result);

			Statement missingProj = new();
			missingProj.SetNamespace(SuiteHelpers.ns);
			missingProj.SetSetName(Set);
			missingProj.SetBinNames("other");
			missingProj.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			missingProj.SetTopK(2);
			AerospikeException proj = Assert.Throws<AerospikeException>(
				() => Internals.ValidateTopK(missingProj));
			Assert.AreEqual(ResultCode.PARSE_ERROR, proj.Result);
		}

		/// <summary>AM-04: concurrent Top-K queries remain exact (bounded client merge).</summary>
		[TestMethod]
		public void ConcurrentTopKQueriesAreExact()
		{
			RequireCapableCluster();

			const int n = 200;
			for (int i = 0; i < n; i++)
			{
				client.Put(null, new Key(SuiteHelpers.ns, Set, "c" + i), new Bin(Bin, i));
			}

			try
			{
				long[] expected = new long[10];
				for (int i = 0; i < 10; i++)
				{
					expected[i] = n - 1 - i;
				}

				Exception failure = null;
				Thread[] threads = new Thread[8];
				for (int t = 0; t < threads.Length; t++)
				{
					threads[t] = new Thread(() =>
					{
						try
						{
							for (int q = 0; q < 5; q++)
							{
								Statement stmt = TopKStatement();
								stmt.SetTopK(10);
								List<long> values = new();
								using RecordSet rs = client.Query(null, stmt);
								while (rs.Next())
								{
									values.Add(rs.Record.GetLong(Bin));
								}
								CollectionAssert.AreEqual(expected, values.ToArray());
							}
						}
						catch (Exception e)
						{
							Interlocked.CompareExchange(ref failure, e, null);
						}
					});
					threads[t].Start();
				}
				foreach (Thread thread in threads)
				{
					thread.Join();
				}
				if (failure != null)
				{
					throw failure;
				}
			}
			finally
			{
				for (int i = 0; i < n; i++)
				{
					client.Delete(null, new Key(SuiteHelpers.ns, Set, "c" + i));
				}
			}
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
	/// Wire-level LUT field parsing (FieldType 14) used by Top-K merge.
	/// </summary>
	[TestClass]
	public class TestLutFieldParsing : TestSync
	{
		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			// Wire LUT parse is pure client; no server feature required.
		}

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
