/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Specialized;

namespace Aerospike.Test
{
	[TestClass]
	public class TestListExp : TestSync
	{
		private readonly bool InstanceFieldsInitialized = false;

		public TestListExp()
		{
			if (!InstanceFieldsInitialized)
			{
				InitializeInstanceFields();
				InstanceFieldsInitialized = true;
			}
		}

		private void InitializeInstanceFields()
		{
			keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, binA);
			keyB = new Key(SuiteHelpers.ns, SuiteHelpers.set, binB);
		}

		private readonly string binA = "A";
		private readonly string binB = "B";
		private readonly string binC = "C";

		private Key keyA;
		private Key keyB;

		private Policy policy;

		[TestInitialize()]
		public void SetUp()
		{
			client.Delete(null, keyA);
			client.Delete(null, keyB);
			policy = new Policy();
		}

		[TestMethod]
		public void ModifyWithContext()
		{
			IList<Value> listSubA =
			[
				Value.Get("e"),
				Value.Get("d"),
				Value.Get("c"),
				Value.Get("b"),
				Value.Get("a"),
			];

			IList<Value> listA =
			[
				Value.Get("a"),
				Value.Get("b"),
				Value.Get("c"),
				Value.Get("d"),
				Value.Get(listSubA),
			];

			IList<Value> listB = [Value.Get("x"), Value.Get("y"), Value.Get("z")];

			client.Operate(null, keyA,
				ListOperation.AppendItems(ListPolicy.Default, binA, (IList)listA),
				ListOperation.AppendItems(ListPolicy.Default, binB, (IList)listB),
				Operation.Put(new Bin(binC, "M"))
				);

			CTX ctx = CTX.ListIndex(4);
			Record record;
			IList result;

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size(
						// Temporarily Append binB/binC to binA in expression.
						ListExp.AppendItems(ListPolicy.Default, Exp.ListBin(binB),
							ListExp.Append(ListPolicy.Default, Exp.StringBin(binC), Exp.ListBin(binA), ctx),
							ctx),
						ctx),
					Exp.Val(9)));

			record = client.Get(policy, keyA, binA);
			AssertRecordFound(keyA, record);

			result = record.GetList(binA);
			Assert.AreEqual(5, result.Count);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size(
						// Temporarily Append local listB and local "M" string to binA in expression.
						ListExp.AppendItems(ListPolicy.Default, Exp.Val((IList)listB),
							ListExp.Append(ListPolicy.Default, Exp.Val("M"), Exp.ListBin(binA), ctx),
							ctx),
						ctx),
					Exp.Val(9)));

			record = client.Get(policy, keyA, binA);
			AssertRecordFound(keyA, record);

			result = record.GetList(binA);
			Assert.AreEqual(5, result.Count);
		}

		[TestMethod]
		public void ExpReturnsList()
		{
			List<Value> list = [Value.Get("a"), Value.Get("b"), Value.Get("c"), Value.Get("d")];

			Expression exp = Exp.Build(Exp.Val(list));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read("var", exp, ExpReadFlags.DEFAULT)
				);

			IList results = record.GetList(binC);
			Assert.AreEqual(2, results.Count);

			IList rlist = (IList)results[1];
			Assert.AreEqual(4, rlist.Count);

			IList results2 = record.GetList("var");
			Assert.AreEqual(4, results2.Count);
		}

		[TestMethod]
		public void NestedMapLiteralPacksCanonical()
		{
			ListDictionary inner = new()
			{
				{ "z", 26L },
				{ "a", 1L }
			};

			ListDictionary innerSorted = new()
			{
				{ "a", 1L },
				{ "z", 26L }
			};

			CollectionAssert.AreEqual(
				Exp.Build(Exp.Val((IList)new List<object> { 0L, inner })).Bytes,
				Exp.Build(Exp.Val((IList)new List<object> { 0L, innerSorted })).Bytes);

			ListDictionary outer = new()
			{
				{ "k", inner }
			};

			ListDictionary outerSorted = new()
			{
				{ "k", innerSorted }
			};

			CollectionAssert.AreEqual(
				Exp.Build(Exp.Val(outer, MapOrder.UNORDERED)).Bytes,
				Exp.Build(Exp.Val(outerSorted, MapOrder.UNORDERED)).Bytes);
		}

		[TestMethod]
		public void OperationPathPreservesInsertionOrder()
		{
			ListDictionary map = new()
			{
				{ "z", 26L },
				{ "a", 1L }
			};

			ListDictionary reversed = new()
			{
				{ "a", 1L },
				{ "z", 26L }
			};

			Assert.IsFalse(Packer.Pack(map, MapOrder.UNORDERED)
				.SequenceEqual(Packer.Pack(reversed, MapOrder.UNORDERED)));
		}

		[TestMethod]
		public void AppendItemsUnsortedMapLiteral()
		{
			// CLIENT-5039: server 8.1.2.3+ (AER-6930) rejects expression map literals
			// that are not in canonical (key sorted) form.
			client.Operate(null, keyA,
				ListOperation.AppendItems(ListPolicy.Default, binA,
					(IList)new List<Value> { Value.Get(0), Value.Get(1) }));

			// ListDictionary is an unordered map with a deterministic, deliberately
			// unsorted iteration order.
			ListDictionary map = new()
			{
				{ "zz", 4L },
				{ "aa", 1L },
				{ "mm", 2L },
				{ "cc", 3L }
			};

			Expression exp = Exp.Build(
				ListExp.Size(
					ListExp.AppendItems(ListPolicy.Default, Exp.Val((IList)new List<object> { map }),
						Exp.ListBin(binA))));

			Record record = client.Operate(null, keyA,
				ExpOperation.Read("result", exp, ExpReadFlags.DEFAULT));

			Assert.AreEqual(3L, record.GetLong("result"));
		}

		[TestMethod]
		public void AppendItemsUnsortedIntKeyMapLiteral()
		{
			// Exact CLIENT-5039 ticket repro: integer keys in non-sorted order.
			client.Operate(null, keyA,
				ListOperation.AppendItems(ListPolicy.Default, binA,
					(IList)new List<Value> { Value.Get(0), Value.Get(1) }));

			ListDictionary map = new()
			{
				{ 1402L, 1802L },
				{ 2003L, 3946L },
				{ 834L, 1374L },
				{ 3117L, 1295L }
			};

			Expression exp = Exp.Build(
				ListExp.AppendItems(ListPolicy.Default, Exp.Val((IList)new List<object> { map }),
					Exp.ListBin(binA)));

			Record record = client.Operate(null, keyA,
				ExpOperation.Read("result", exp, ExpReadFlags.DEFAULT));

			Assert.AreEqual(3, record.GetList("result").Count);
		}

		[TestMethod]
		public void AppendItemsOperationUnsortedMap()
		{
			ListDictionary map = new()
			{
				{ "z", 26L },
				{ "a", 1L },
				{ "m", 13L }
			};

			// Non-expression (operation) packing keeps insertion order and is not
			// canonicalized.
			client.Operate(null, keyB,
				ListOperation.AppendItems(ListPolicy.Default, binB,
					(IList)new List<Value> { Value.Get(0), Value.Get(map) }));

			Record record = client.Get(null, keyB, binB);
			AssertRecordFound(keyB, record);
			Assert.AreEqual(2, record.GetList(binB).Count);
		}

		[TestMethod]
		public void JoinWithoutSeparator()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "list join");

			IList<Value> items = [Value.Get("alpha"), Value.Get("beta"), Value.Get("gamma")];
			client.Put(null, keyA, new Bin(binA, items));

			Expression exp = Exp.Build(ListExp.Join(Exp.ListBin(binA)));
			Record record = client.Operate(null, keyA,
				ExpOperation.Read("result", exp, ExpReadFlags.DEFAULT));

			Assert.AreEqual("alphabetagamma", record.GetString("result"));
		}

		[TestMethod]
		public void JoinWithSeparator()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "list join");

			IList<Value> items = [Value.Get("alpha"), Value.Get("beta"), Value.Get("gamma")];
			client.Put(null, keyA, new Bin(binA, items));

			Expression exp = Exp.Build(ListExp.Join(Exp.Val(", "), Exp.ListBin(binA)));
			Record record = client.Operate(null, keyA,
				ExpOperation.Read("result", exp, ExpReadFlags.DEFAULT));

			Assert.AreEqual("alpha, beta, gamma", record.GetString("result"));
		}

		[TestMethod]
		public void JoinOnNestedListViaContext()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "list join");

			IList<Value> inner = [Value.Get("x"), Value.Get("y")];
			IList<Value> outer = [Value.Get("skip"), Value.Get(inner)];
			client.Put(null, keyA, new Bin(binA, outer));

			Expression exp = Exp.Build(
				ListExp.Join(Exp.Val("-"), Exp.ListBin(binA), CTX.ListIndex(1)));
			Record record = client.Operate(null, keyA,
				ExpOperation.Read("result", exp, ExpReadFlags.DEFAULT));

			Assert.AreEqual("x-y", record.GetString("result"));
		}
	}
}
