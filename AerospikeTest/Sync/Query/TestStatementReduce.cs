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
	/// White-box unit tests for Statement.SetReduce / ResolveReduce internals that back
	/// the public SetOrderBy / SetTopK sugar. Mirrors Java TestStatementReduce.
	/// </summary>
	[TestClass]
	public class TestStatementReduce
	{
		private const string Ns = "test";
		private const string Set = "reduceset";
		private const string Bin = "val";

		private static Key Key(string userKey) => new(Ns, Set, userKey);

		private static Record Record(string binName, object value) =>
			new(new Dictionary<string, object> { { binName, value } }, 1, 0);

		[TestMethod]
		public void StatementDefaultsToNoReduce()
		{
			Statement stmt = new();
			Assert.IsNull(Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementResolvesSingleTopK()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt, Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(Record(Bin, 1L), Key("k1"));
			resolved.AcceptPartial(Record(Bin, 2L), Key("k2"));
			Assert.AreEqual(2, resolved.GetResult().Length);
		}

		[TestMethod]
		public void StatementComposesSplitOrderByAndLimit()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
				Reduce.Limit(Bin, 2));

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(Record(Bin, 1L), Key("k1"));
			resolved.AcceptPartial(Record(Bin, 5L), Key("k2"));
			resolved.AcceptPartial(Record(Bin, 3L), Key("k3"));

			Record[] result = resolved.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(5L, result[0].GetLong(Bin));
			Assert.AreEqual(3L, result[1].GetLong(Bin));
		}

		[TestMethod]
		public void StatementSplitOrderIndependent()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.Limit(Bin, 2),
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE));

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(Record(Bin, 3L), Key("k1"));
			resolved.AcceptPartial(Record(Bin, 1L), Key("k2"));

			Record[] result = resolved.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(1L, result[0].GetLong(Bin));
		}

		[TestMethod]
		public void StatementResolveReduceIsMemoized()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
				Reduce.Limit(Bin, 2));

			Assert.AreSame(Internals.ResolveReduce(stmt), Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementSetReduceResetsMemoizedResolution()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt, Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));
			ReduceSpec<Record, Record> first = Internals.ResolveReduce(stmt);

			Internals.SetReduce(stmt, Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3));
			Assert.AreNotSame(first, Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementResolveReduceOrderByWithoutLimitThrows()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt, Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE));
			Assert.Throws<AerospikeException>(() => Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementResolveReduceComposeBinMismatchThrows()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.OrderBy("binA", BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
				Reduce.Limit("binB", 2));
			Assert.Throws<AerospikeException>(() => Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementResolveReduceDuplicateOrderByThrows()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE),
				Reduce.Limit(Bin, 2));
			Assert.Throws<AerospikeException>(() => Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void StatementResolveReduceDuplicateLimitThrows()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt,
				Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
				Reduce.Limit(Bin, 2),
				Reduce.Limit(Bin, 3));
			Assert.Throws<AerospikeException>(() => Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void SetReduceEmptyClearsReduce()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt, Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));
			Assert.IsNotNull(Internals.ResolveReduce(stmt));

			Internals.SetReduce(stmt);
			Assert.IsNull(Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void SetOrderByThenSetTopKResolvesToTopK()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.DOUBLE, Order.ASC);
			stmt.SetTopK(2);

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(Record(Bin, 3.0), Key("k1"));
			resolved.AcceptPartial(Record(Bin, 1.0), Key("k2"));
			resolved.AcceptPartial(Record(Bin, 2.0), Key("k3"));

			Record[] result = resolved.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(1.0, result[0].GetDouble(Bin), 0.0001);
			Assert.AreEqual(2.0, result[1].GetDouble(Bin), 0.0001);
		}

		[TestMethod]
		public void SetOrderByWithFlagsThenSetTopKResolvesToTopK()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
			stmt.SetTopK(2);

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(Record(Bin, "banana"), Key("k1"));
			resolved.AcceptPartial(Record(Bin, "Apple"), Key("k2"));

			Record[] result = resolved.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual("Apple", result[0].GetString(Bin));
		}

		[TestMethod]
		public void SetTopKOverridesPriorReduce()
		{
			Statement stmt = new();
			Internals.SetReduce(stmt, Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 5));
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);

			Internals.ResolveReduce(stmt).AcceptPartial(Record(Bin, 1L), Key("k1"));
			Assert.AreEqual(1, Internals.ResolveReduce(stmt).GetResult().Length);
		}

		[TestMethod]
		public void TopKRejectsInvalidWireSpecification()
		{
			Statement flags = new();
			flags.SetOrderBy(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
			flags.SetTopK(1);
			Assert.Throws<AerospikeException>(() => flags.ValidateTopK());

			Statement projection = new();
			projection.SetBinNames("other");
			projection.SetOrderBy(Bin, BinDataType.INTEGER, Order.ASC);
			projection.SetTopK(1);
			Assert.Throws<AerospikeException>(() => projection.ValidateTopK());

			Statement name = new();
			name.SetOrderBy("sixteen-byte-bin", BinDataType.INTEGER, Order.ASC);
			name.SetTopK(1);
			Assert.Throws<AerospikeException>(() => name.ValidateTopK());

			Statement duplicateOps = new();
			duplicateOps.Operations = [Operation.Get(Bin), Operation.Get(Bin)];
			duplicateOps.SetOrderBy(Bin, BinDataType.INTEGER, Order.ASC);
			duplicateOps.SetTopK(1);
			AerospikeException dup = Assert.Throws<AerospikeException>(() => duplicateOps.ValidateTopK());
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, dup.Result);
			Assert.IsTrue(dup.Message.Contains("exactly one projection operation"));
		}

		[TestMethod]
		public void OrderByWithoutTopKIsVisibleOnStatement()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);

			Assert.IsTrue(stmt.HasOrderBy);
			Assert.IsFalse(stmt.HasTopK);
			Assert.AreEqual(Bin, stmt.TopKBin);
			Assert.AreEqual(BinDataType.INTEGER, stmt.TopKType);
			Assert.AreEqual(Order.DESC, stmt.TopKOrder);
		}

		[TestMethod]
		public void SetOrderByKeepsTopKLimit()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(3);
			stmt.SetOrderBy("other", BinDataType.INTEGER, Order.ASC);

			Assert.IsTrue(stmt.HasTopK);
			Assert.IsTrue(stmt.HasOrderBy);
			Assert.AreEqual(3, stmt.TopKLimit);
			Assert.AreEqual("other", stmt.TopKBin);
			Assert.IsFalse(stmt.HasTopKThen);
		}

		[TestMethod]
		public void ThenOrderByAddsSecondKeyAndKeepsLimit()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(5);
			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);

			Assert.IsTrue(stmt.HasTopK);
			Assert.AreEqual(5, stmt.TopKLimit);
			Assert.AreEqual(Bin, stmt.TopKBin);
			Assert.IsTrue(stmt.HasTopKThen);
			Assert.AreEqual("ts", stmt.TopKThenBin);

			ReduceSpec<Record, Record> resolved = Internals.ResolveReduce(stmt);
			resolved.AcceptPartial(
				new Record(new Dictionary<string, object> { { Bin, 10L }, { "ts", 2L } }, 1, 0),
				Key("a"));
			resolved.AcceptPartial(
				new Record(new Dictionary<string, object> { { Bin, 10L }, { "ts", 1L } }, 1, 0),
				Key("b"));
			resolved.AcceptPartial(
				new Record(new Dictionary<string, object> { { Bin, 9L }, { "ts", 0L } }, 1, 0),
				Key("c"));

			Record[] result = resolved.GetResult();
			Assert.AreEqual(5, stmt.TopKLimit);
			Assert.AreEqual(3, result.Length);
			Assert.AreEqual(10L, result[0].GetLong(Bin));
			Assert.AreEqual(1L, result[0].GetLong("ts"));
			Assert.AreEqual(10L, result[1].GetLong(Bin));
			Assert.AreEqual(2L, result[1].GetLong("ts"));
		}

		[TestMethod]
		public void ThenOrderByThirdKeyThrows()
		{
			Statement stmt = new();
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);
			Assert.Throws<AerospikeException>(() =>
				stmt.ThenOrderBy("id", BinDataType.INTEGER, Order.ASC));
		}

		[TestMethod]
		public void ThenOrderByBeforeSetOrderByDoesNotThrow()
		{
			Statement stmt = new();
			stmt.ThenOrderBy("ts", BinDataType.INTEGER, Order.ASC);
			stmt.SetOrderBy(Bin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(2);
			Assert.AreEqual(Bin, stmt.TopKBin);
			Assert.IsFalse(stmt.HasTopKThen);
		}
	}
}
