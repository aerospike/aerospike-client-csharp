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
	}
}
