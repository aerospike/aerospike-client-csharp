/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Collections.Specialized;

namespace Aerospike.Test
{
	[TestClass]
	public class TestMapExp : TestSync
	{
		private readonly bool InstanceFieldsInitialized = false;

		public TestMapExp()
		{
			if (!InstanceFieldsInitialized)
			{
				InitializeInstanceFields();
				InstanceFieldsInitialized = true;
			}
		}

		private void InitializeInstanceFields()
		{
			key = new Key(SuiteHelpers.ns, SuiteHelpers.set, bin);
		}

		private readonly string bin = "m";

		private Key key;

		private Policy policy;

		[TestInitialize()]
		public void SetUp()
		{
			client.Delete(null, key);
			policy = new Policy();
		}

		[TestMethod]
		public void PutSortedDictionary()
		{
			var map = new SortedDictionary<string, string>
			{
				["key1"] = "e",
				["key2"] = "d",
				["key3"] = "c",
				["key4"] = "b",
				["key5"] = "a"
			};

			client.Operate(null, key,
				MapOperation.PutItems(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT), bin, map)
				);

			policy.filterExp = Exp.Build(Exp.EQ(Exp.MapBin("m"), Exp.Val(map, MapOrder.KEY_ORDERED)));

			Record record = client.Get(policy, key, bin);
			AssertRecordFound(key, record);
		}

		[TestMethod]
		public void UnsortedMapLiteralPacksCanonical()
		{
			ListDictionary unsorted = new()
			{
				{ "z", 26L },
				{ 5L, "five" },
				{ "a", 1L },
				{ -3L, "neg" }
			};

			ListDictionary sorted = new()
			{
				{ -3L, "neg" },
				{ 5L, "five" },
				{ "a", 1L },
				{ "z", 26L }
			};

			CollectionAssert.AreEqual(
				Exp.Build(Exp.Val(unsorted, MapOrder.UNORDERED)).Bytes,
				Exp.Build(Exp.Val(sorted, MapOrder.UNORDERED)).Bytes);
		}

		[TestMethod]
		public void UnsortedMapLiteral()
		{
			// CLIENT-5039: unordered multi-key map literals must be canonicalized or
			// server 8.1.2.3+ (AER-6930) rejects the expression with PARAMETER_ERROR.
			// ListDictionary is an unordered map with a deterministic, deliberately
			// unsorted iteration order.
			ListDictionary map = new()
			{
				{ "key5", "a" },
				{ "key1", "e" },
				{ "key4", "b" },
				{ "key2", "d" },
				{ "key3", "c" }
			};

			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "usml");
			client.Put(null, key, new Bin("m", "unused"));

			Expression exp = Exp.Build(MapExp.Size(Exp.Val(map, MapOrder.UNORDERED)));

			Record rec = client.Operate(null, key, ExpOperation.Read("sz", exp, ExpReadFlags.DEFAULT));
			AssertRecordFound(key, rec);
			Assert.AreEqual(5L, rec.GetLong("sz"));
		}

		[TestMethod]
		public void NestedUnsortedMapLiteral()
		{
			ListDictionary inner = new()
			{
				{ 1402L, 1802L },
				{ 834L, 1374L }
			};

			ListDictionary map = new()
			{
				{ "z", inner },
				{ "a", 1L }
			};

			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "nusml");
			client.Put(null, key, new Bin("m", "unused"));

			// Look up the nested map inside the literal — the whole literal, at every
			// depth, must be in canonical form for the server to accept it.
			Expression exp = Exp.Build(
				MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.MAP, Exp.Val("z"), Exp.Val(map, MapOrder.UNORDERED)));

			Record rec = client.Operate(null, key, ExpOperation.Read("res", exp, ExpReadFlags.DEFAULT));
			AssertRecordFound(key, rec);

			var res = rec.GetMap("res");
			Assert.AreEqual(2, res.Count);
			Assert.AreEqual(1802L, res[1402L]);
			Assert.AreEqual(1374L, res[834L]);
		}

		[TestMethod]
		public void InvertedMapExp()
		{
			var map = new Dictionary<string, int>
			{
				["a"] = 1,
				["b"] = 2,
				["c"] = 2,
				["d"] = 3
			};

			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "ime");
			Bin bin = new("m", map);

			client.Put(null, key, bin);

			// Use INVERTED to return map with entries removed where value != 2
			Expression e = Exp.Build(MapExp.RemoveByValue(MapReturnType.INVERTED, Exp.Val(2), Exp.MapBin(bin.name)));

			Record rec = client.Operate(null, key, ExpOperation.Read(bin.name, e, ExpReadFlags.DEFAULT));
			AssertRecordFound(key, rec);

			var m = rec.GetMap(bin.name);
			Assert.AreEqual((long)2, m.Count);
			Assert.AreEqual((long)2, m["b"]);
			Assert.AreEqual((long)2, m["c"]);
		}
	}
}
