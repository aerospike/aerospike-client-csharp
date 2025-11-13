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
	public class TestCDTExp : TestSync
	{
		[TestMethod]
		public void TestCDTExpSelect()
		{
			Key keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtExpSelectKey");

			try
			{
				client.Delete(null, keyA);
			}
			catch (Exception)
			{
			}

			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "title", "Sayings of the Century" },
				{ "price", 10.45 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Sword of Honour" },
				{ "price", 20.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Moby Dick" },
				{ "price", 5.01 }
			};
			booksList.Add(book3);

			Dictionary<string, object> book4 = [];
			book4.Add("title", "The Lord of the Rings");
			book4.Add("price", 30.98);
			booksList.Add(book4);

			Dictionary<string, object> rootMap = [];
			rootMap.Add("book", booksList);

			Bin bin = new Bin("res1", rootMap);
			client.Put(null, keyA, bin);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,                   // Return type: list
					(int)LoopVarPart.VALUE,          // AS_CDT_SELECT_LEAF_MAP_VALUE equivalent
					Exp.MapBin("res1"),              // Source bin
					bookKey, allChildren, priceKey   // CTX path
				)
			);

			Record result = client.Operate(null, keyA,
				ExpOperation.Write("A", selectExp, ExpWriteFlags.DEFAULT)
			);

			// CDT select expression operation should succeed
			Assert.IsNotNull(result);

			Record finalRecord = client.Get(null, keyA);
			var priceList = finalRecord.GetList("A");

			// Price list should exist
			Assert.IsNotNull(priceList);
			Assert.AreEqual(4, priceList.Count);

			double firstPrice = (double)priceList[0];
			Assert.IsTrue(firstPrice < 11);
		}

		[TestMethod]
		public void TestCDTExpApply()
		{
			Key keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtExpApplyKey");

			try
			{
				client.Delete(null, keyA);
			}
			catch (Exception)
			{
			}

			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "title", "Sayings of the Century" },
				{ "price", 10.45 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Sword of Honour" },
				{ "price", 20.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Moby Dick" },
				{ "price", 5.01 }
			};
			booksList.Add(book3);

			Dictionary<string, object> book4 = new()
			{
				{ "title", "The Lord of the Rings" },
				{ "price", 30.98 }
			};
			booksList.Add(book4);

			Dictionary<string, object> rootMap = [];
			rootMap.Add("book", booksList);

			Bin bin = new Bin("res1", rootMap);
			client.Put(null, keyA, bin);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Exp modifyExp = Exp.Mul(
				Exp.LoopVarFloat(LoopVarPart.VALUE),  // Current price value
				Exp.Val(1.50)                         // Multiply by 1.50
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,                     // Return type: map
					0,                                // Flags
					modifyExp,                        // Modify expression
					Exp.MapBin("res1"),              // Source bin
					bookKey, allChildren, priceKey   // CTX path
				)
			);

			Record result = client.Operate(null, keyA,
				ExpOperation.Write("res1", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// CDT apply expression operation should succeed
			Assert.IsNotNull(result);

			Record finalRecord = client.Get(null, keyA);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue("res1");
			// Root map should exist
			Assert.IsNotNull(finalRootMap);

			List<object> finalBooksList = (List<object>)finalRootMap["book"];
			Assert.IsTrue(finalBooksList != null && finalBooksList.Count > 0);

			Dictionary<object, object> firstBook = (Dictionary<object, object>)finalBooksList[0];
			Assert.IsNotNull(firstBook);

			object priceObj = firstBook["price"];
			Assert.IsNotNull(priceObj);

			double finalPrice = (double)priceObj;
			Assert.IsTrue(finalPrice > 11.0);

			double expectedPrice = 10.45 * 1.50;
			Assert.IsTrue(Math.Abs(finalPrice - expectedPrice) < 0.01);
		}
	}
}
