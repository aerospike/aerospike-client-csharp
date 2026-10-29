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
	/// Verifies Top-K results against a live cluster in client-only and pushdown modes.
	/// </summary>
	[TestClass]
	public class TestTopKPushdown : TestSync
	{
		private const string Set = "topKPushdown";
		private const string Bin = "rank";
		private const string Derived = "derived";
		private const int Count = 100;

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			for (int i = 1; i <= Count; i++)
			{
				client.Put(null, new Key(SuiteHelpers.ns, Set, "key" + i), new Bin(Bin, i));
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			for (int i = 1; i <= Count; i++)
			{
				client.Delete(null, new Key(SuiteHelpers.ns, Set, "key" + i));
			}
		}

		private static void RequireCapableCluster()
		{
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

		[TestMethod]
		public void ClientOnlyReturnsGlobalTopK()
		{
			RequireCapableCluster();

			CollectionAssert.AreEqual(new long[] { 100, 99, 98 }, Values(ClientOnly(Order.DESC, 3), Bin));
			CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Values(ClientOnly(Order.ASC, 3), Bin));
		}

		[TestMethod]
		public void PushdownReturnsGlobalTopK()
		{
			RequireCapableCluster();

			CollectionAssert.AreEqual(new long[] { 100, 99, 98 }, Values(Pushdown(Order.DESC, 3), Bin));
			CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Values(Pushdown(Order.ASC, 3), Bin));
		}

		[TestMethod]
		public void BothModesAreEquivalent()
		{
			RequireCapableCluster();

			foreach (Order order in new[] { Order.ASC, Order.DESC })
			{
				foreach (int k in new[] { 1, 5, 25 })
				{
					long[] clientOnly = Values(ClientOnly(order, k), Bin);
					long[] pushdown = Values(Pushdown(order, k), Bin);

					Assert.AreEqual(k, pushdown.Length);
					CollectionAssert.AreEqual(clientOnly, pushdown, $"Mismatch for order={order} k={k}");
				}
			}
		}

		[TestMethod]
		public void ExpressionProjectedKeyIsEquivalent()
		{
			RequireCapableCluster();

			int k = 5;
			long[] expected = new long[k];

			for (int i = 0; i < k; i++)
			{
				expected[i] = (i + 1) * 10L;
			}

			CollectionAssert.AreEqual(expected, Values(ClientOnlyDerived(Order.ASC, k), Derived));
			CollectionAssert.AreEqual(expected, Values(PushdownDerived(Order.ASC, k), Derived));
		}

		/// <summary>
		/// Order-by without top-k is sent unchanged; a vector-capable server rejects
		/// with <see cref="ResultCode.PARAMETER_ERROR"/> and
		/// <see cref="SubCode.PARAM_TOP_K_INVALID"/> when error details are requested.
		/// </summary>
		[TestMethod]
		public void OrderByWithoutTopKReturnsParamTopKInvalid()
		{
			RequireCapableCluster();

			AerospikeException ae = QueryOrderByWithoutTopK(errorDetailVerbosity: 2);

			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
			Assert.AreEqual(SubCode.PARAM_TOP_K_INVALID, ae.SubCode);
			Assert.IsTrue(ae.BaseMessage.Contains("top-k needs both order-by and top-k fields"),
				"Expected R20 detail in: " + ae.BaseMessage);
			string prefix = "Error " + ResultCode.PARAMETER_ERROR + "," + SubCode.PARAM_TOP_K_INVALID;
			Assert.IsTrue(ae.Message.StartsWith(prefix), "Expected message to start with \"" + prefix + "\": " + ae.Message);
		}

		/// <summary>
		/// With error-detail verbosity disabled, the same rejection is a plain
		/// parameter error: no subcode and the default result-code string.
		/// </summary>
		[TestMethod]
		public void OrderByWithoutTopKWithoutErrorDetails()
		{
			RequireCapableCluster();

			AerospikeException ae = QueryOrderByWithoutTopK(errorDetailVerbosity: 0);

			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			Assert.AreEqual(ResultCode.GetResultString(ResultCode.PARAMETER_ERROR), ae.BaseMessage);
		}

		private static AerospikeException QueryOrderByWithoutTopK(int errorDetailVerbosity)
		{
			Statement statement = Base(Bin);
			statement.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);

			QueryPolicy policy = new()
			{
				errorDetailVerbosity = errorDetailVerbosity
			};

			try
			{
				using RecordSet recordSet = client.Query(policy, statement);
				while (recordSet.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for order-by without top-k");
				return null;
			}
			catch (AerospikeException ae)
			{
				return ae;
			}
		}

		[TestMethod]
		public void MissingOrderKeyRanksLast()
		{
			RequireCapableCluster();

			Key[] missing =
			[
				new Key(SuiteHelpers.ns, Set, "missing1"),
				new Key(SuiteHelpers.ns, Set, "missing2")
			];

			foreach (Key key in missing)
			{
				client.Put(null, key, new Bin("marker", -1));
			}

			try
			{
				int k = Count + missing.Length;

				foreach (Order order in new[] { Order.ASC, Order.DESC })
				{
					Statement byBinName = Pushdown(order, k);
					Statement byOperation = Pushdown(order, k);
					byOperation.BinNames = null;
					byOperation.Operations = [Operation.Get(Bin)];

					foreach (Statement statement in new[] { byBinName, byOperation })
					{
						List<object> ranked = RankedValues(statement, Bin);
						Assert.AreEqual(k, ranked.Count);

						for (int i = 0; i < Count; i++)
						{
							long expected = order == Order.ASC ? i + 1 : Count - i;
							Assert.AreEqual(expected, ranked[i]);
						}
						Assert.IsNull(ranked[Count]);
						Assert.IsNull(ranked[Count + 1]);
					}
				}
			}
			finally
			{
				foreach (Key key in missing)
				{
					client.Delete(null, key);
				}
			}
		}

		private static Statement ClientOnly(Order order, int k)
		{
			Statement statement = Base(Bin);
			Internals.SetReduce(statement, Reduce.TopK(Bin, BinDataType.INTEGER, order, OrderByFlags.NONE, k));
			return statement;
		}

		private static Statement Pushdown(Order order, int k)
		{
			Statement statement = Base(Bin);
			statement.SetOrderBy(Bin, BinDataType.INTEGER, order);
			statement.SetTopK(k);
			return statement;
		}

		private static Statement ClientOnlyDerived(Order order, int k)
		{
			Statement statement = DerivedStatement();
			Internals.SetReduce(statement, Reduce.TopK(Derived, BinDataType.INTEGER, order, OrderByFlags.NONE, k));
			return statement;
		}

		private static Statement PushdownDerived(Order order, int k)
		{
			Statement statement = DerivedStatement();
			statement.SetOrderBy(Derived, BinDataType.INTEGER, order);
			statement.SetTopK(k);
			return statement;
		}

		private static Statement Base(string bin)
		{
			Statement statement = new();
			statement.SetNamespace(SuiteHelpers.ns);
			statement.SetSetName(Set);
			statement.SetBinNames(bin);
			return statement;
		}

		private static Statement DerivedStatement()
		{
			Statement statement = new();
			statement.SetNamespace(SuiteHelpers.ns);
			statement.SetSetName(Set);
			statement.Operations =
			[
				ExpOperation.Read(Derived,
					Exp.Build(Exp.Mul(Exp.IntBin(Bin), Exp.Val(10))),
					ExpReadFlags.DEFAULT)
			];
			return statement;
		}

		private static long[] Values(Statement statement, string valueBin)
		{
			List<long> values = new();
			RecordSet recordSet = client.Query(null, statement);

			try
			{
				while (recordSet.Next())
				{
					values.Add(recordSet.Record.GetLong(valueBin));
				}
			}
			finally
			{
				recordSet.Close();
			}

			return values.ToArray();
		}

		private static List<object> RankedValues(Statement statement, string valueBin)
		{
			List<object> values = new();
			RecordSet recordSet = client.Query(null, statement);

			try
			{
				while (recordSet.Next())
				{
					values.Add(recordSet.Record.GetValue(valueBin));
				}
			}
			finally
			{
				recordSet.Close();
			}

			return values;
		}
	}
}
