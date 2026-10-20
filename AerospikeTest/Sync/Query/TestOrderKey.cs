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
	/// OrderKey behavior covered through the public Top-K reducer (OrderKey is internal).
	/// Mirrors Java <c>com.aerospike.client.query.TestOrderKey</c>.
	/// </summary>
	[TestClass]
	public class TestOrderKey
	{
		private const string Bin = "bin";
		private const string Ns = "test";
		private const string Set = "set";

		private static Key Key(string userKey) => new(Ns, Set, userKey);

		private static Record Record(object value, int generation = 1) =>
			new(new Dictionary<string, object> { { Bin, value } }, generation, 0);

		[TestMethod]
		public void NegativeZeroEqualsPositiveZero()
		{
			// PRD R29 / reduce-design: −0.0 equals 0.0; tie breaks by digest.
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, 2);
			reducer.AcceptPartial(Record(0.0), Key("pos"));
			reducer.AcceptPartial(Record(-0.0), Key("neg"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(0.0, result[0].GetDouble(Bin));
			Assert.AreEqual(0.0, result[1].GetDouble(Bin));
		}

		[TestMethod]
		public void NanUsesNaturalOrderBeforeDirection()
		{
			ReduceSpec<Record, Record> asc = Reduce.TopK(Bin, BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, 2);
			asc.AcceptPartial(Record(double.NaN), Key("nan"));
			asc.AcceptPartial(Record(1.0), Key("finite"));
			Record[] ascResult = asc.GetResult();
			Assert.AreEqual(1.0, ascResult[0].GetDouble(Bin));
			Assert.IsTrue(double.IsNaN(ascResult[1].GetDouble(Bin)));

			ReduceSpec<Record, Record> desc = Reduce.TopK(Bin, BinDataType.DOUBLE, Order.DESC, OrderByFlags.NONE, 2);
			desc.AcceptPartial(Record(1.0), Key("finite"));
			desc.AcceptPartial(Record(double.NaN), Key("nan"));
			Record[] descResult = desc.GetResult();
			Assert.IsTrue(double.IsNaN(descResult[0].GetDouble(Bin)));
			Assert.AreEqual(1.0, descResult[1].GetDouble(Bin));
		}

		[TestMethod]
		public void DedupPrefersHigherGeneration()
		{
			// R29: when the same digest arrives twice, keep the newer generation
			// (not the better-ranked order-key value).
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 1);
			Key key = Key("same");

			reducer.AcceptPartial(Record(10L, generation: 1), key);
			reducer.AcceptPartial(Record(99L, generation: 3), key);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(99L, result[0].GetLong(Bin));
			Assert.AreEqual(3, result[0].generation);
		}
	}
}
