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

		private static Record Record(object value) =>
			new(new Dictionary<string, object> { { Bin, value } }, 1, 0);

		[TestMethod]
		public void CaseInsensitiveStringUsesAsciiFolding()
		{
			// U+212A (Kelvin) lowercases to 'k' in Unicode, but order-by ASCII folding
			// only maps A-Z. So "K" folds to 0x6B and sorts before the multi-byte Kelvin.
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE, 2);
			reducer.AcceptPartial(Record("\u212a"), Key("kelvin"));
			reducer.AcceptPartial(Record("K"), Key("ascii"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual("K", result[0].GetString(Bin));
			Assert.AreEqual("\u212a", result[1].GetString(Bin));
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
	}
}
