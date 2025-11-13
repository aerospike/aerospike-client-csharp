/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	[TestClass]
	public class TestCDTOperate : TestSync
	{
		private readonly string binName = "testbin";

		[TestMethod]
		public void TestCDTOperateWithExpressions()
		{
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 215);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "title", "Sayings of the Century" },
				{ "price", 8.95 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Sword of Honour" },
				{ "price", 12.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Moby Dick" },
				{ "price", 8.99 }
			};
			booksList.Add(book3);

			Dictionary<string, object> book4 = new()
			{
				{ "title", "The Lord of the Rings" },
				{ "price", 22.99 }
			};
			booksList.Add(book4);

			Dictionary<string, object> rootMap = new()
			{
				{ "book", booksList }
			};

			Bin bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			Record record = client.Get(null, rkey);
			// Record should exist
			Assert.IsNotNull(record);

			CTX ctx1 = CTX.AllChildren();
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
						Exp.Val("price"), Exp.LoopVarMap(LoopVarPart.VALUE)),
					Exp.Val(10.0)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.LoopVarString(LoopVarPart.MAP_KEY), Exp.Val("title"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, (int)LoopVarPart.VALUE, ctx1, ctx2, ctx3);

			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null && results.Count > 0)
			{
				Console.WriteLine("Selected titles: " + results);
			}
		}

		[TestMethod]
		public void TestCDTApplyWithExpressions()
		{
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 216);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "title", "Sayings of the Century" },
				{ "price", 8.95 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Sword of Honour" },
				{ "price", 12.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Moby Dick" },
				{ "price", 8.99 }
			};
			booksList.Add(book3);

			Dictionary<string, object> book4 = new()
			{
				{ "title", "The Lord of the Rings" },
				{ "price", 22.99 }
			};
			booksList.Add(book4);

			Dictionary<string, object> rootMap = new()
			{
				{ "book", booksList }
			};

			Bin bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			Record record = client.Get(null, rkey);
			// Record should exist
			Assert.IsNotNull(record);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Expression modifyExp = Exp.Build(
				Exp.Mul(
					Exp.LoopVarFloat(LoopVarPart.VALUE),  // Current price value
					Exp.Val(1.10)                         // Multiply by 1.10
				)
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, 0, modifyExp, bookKey, allChildren, priceKey);

			Record result = client.Operate(null, rkey, applyOp);
			// CDT apply operation should succeed
			Assert.IsNotNull(result);

			Record finalRecord = client.Get(null, rkey);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue(binName);
			// Root map should exist
			Assert.IsNotNull(finalRootMap);

			List<object> finalBooksList = (List<object>)finalRootMap["book"];
			Assert.IsTrue(finalBooksList != null && finalBooksList.Count > 0);

			Dictionary<object, object> firstBook = (Dictionary<object, object>)finalBooksList[0];
			// First book should exist
			Assert.IsNotNull(firstBook);

			object priceObj = firstBook["price"];
			// Price should exist
			Assert.IsNotNull(priceObj);

			double finalPrice = (double)priceObj;
			// Price should be increased (> 9)
			Assert.IsTrue(finalPrice > 9.0);

			double expectedPrice = 8.95 * 1.10;
			Assert.IsTrue(Math.Abs(finalPrice - expectedPrice) < 0.01);
		}
	}
}
