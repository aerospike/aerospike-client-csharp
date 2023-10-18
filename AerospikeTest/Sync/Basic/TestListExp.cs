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
using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

namespace Aerospike.Test
{
	[TestClass]
	public class TestListExp : TestSync
	{
		private bool InstanceFieldsInitialized = false;

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
			keyA = new Key(args.ns, args.set, binA);
			keyB = new Key(args.ns, args.set, binB);
		}

		private string binA = "A";
		private string binB = "B";
		private string binC = "C";

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
			IList<Value> listSubA = new List<Value>();
			listSubA.Add(Value.Get("e"));
			listSubA.Add(Value.Get("d"));
			listSubA.Add(Value.Get("c"));
			listSubA.Add(Value.Get("b"));
			listSubA.Add(Value.Get("a"));

			IList<Value> listA = new List<Value>();
			listA.Add(Value.Get("a"));
			listA.Add(Value.Get("b"));
			listA.Add(Value.Get("c"));
			listA.Add(Value.Get("d"));
			listA.Add(Value.Get(listSubA));

			IList<Value> listB = new List<Value>();
			listB.Add(Value.Get("x"));
			listB.Add(Value.Get("y"));
			listB.Add(Value.Get("z"));

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
			List<Value> list = new List<Value>();
			list.Add(Value.Get("a"));
			list.Add(Value.Get("b"));
			list.Add(Value.Get("c"));
			list.Add(Value.Get("d"));

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
		public void ExpFilterMap()
		{
			List<Key> keys = new();
			for (int i = 0; i < 5; i++)
			{
				Key key = new Key("test", "demo", i);
				keys.Add(key);

				// List element
				// List<String> child = new ArrayList<>() {{
				//     add("hello");
				//     add("world");
				// }};
				// child.add(String.format("%d", i));

				// Map element
				Dictionary<int, int> child = new()
				{
					{ 10, 10 },
					{ 11, 11 }
				};
				child.Add(i, i);

				// Parent List
				List<Object> parent = new()
				{
					"a",
					100,
					child
				};
				client.Put(null, key, new Bin("list_bin", parent));
			}

			// List<String> target = new ArrayList<>(){{
			//     add("hello");
			//     add("world");
			//     add("3");
			// }};
			Dictionary<int, int> target = new()
			{
				{ 10, 10 },
				{ 11, 11 },
				{ 3, 3 }
			};
			List<Object> check = new()
			{
				target
			};


			Expression expr = Exp.Build(
				Exp.EQ(
					ListExp.GetByValue(ListReturnType.VALUE, Exp.Val(target, MapOrder.UNORDERED), Exp.Bin("list_bin", Exp.Type.LIST)),
					Exp.Val(check)
				)
			);

			BatchPolicy bp = new BatchPolicy();
			bp.filterExp = expr;
			bp.failOnFilteredOut = true;

			Record[] records = client.Get(bp, keys.ToArray());
		}
	}
}
