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
	[TestClass]
	public class TestCDTExp : TestSync
	{
		[TestMethod]
		public void TestCDTExpSelect()
		{
			var keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtExpSelectKey");

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

			var bin = new Bin("res1", rootMap);
			client.Put(null, keyA, bin);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,                   // Return type: list
					SelectFlag.VALUE,                // AS_CDT_SELECT_LEAF_MAP_VALUE equivalent
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

			// Price list should exist and have 4 prices
			Assert.IsNotNull(priceList);
			Assert.AreEqual(4, priceList.Count);

			// First price should be < 11
			double firstPrice = (double)priceList[0];
			Assert.IsTrue(firstPrice < 11);
		}

		[TestMethod]
		public void TestCDTExpApply()
		{
			var keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtExpApplyKey");

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

			var bin = new Bin("res1", rootMap);
			client.Put(null, keyA, bin);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Exp modifyExp = Exp.Mul(
				Exp.FloatLoopVar(LoopVarPart.VALUE),  // Current price value
				Exp.Val(1.50)                         // Multiply by 1.50
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,                    // Return type: map
					ModifyFlag.DEFAULT,              // Flags
					modifyExp,                       // Modify expression
					Exp.MapBin("res1"),              // Source bin
					bookKey, allChildren, priceKey   // CTX path
				)
			);

			Record result = client.Operate(null, keyA,
				ExpOperation.Write("res1", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// CDT apply expression operation should succeed
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, keyA);
			Assert.IsNotNull(finalRecord);

			// Root map should exist
			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue("res1");
			Assert.IsNotNull(finalRootMap);

			// Books list should exist
			List<object> finalBooksList = (List<object>)finalRootMap["book"];
			Assert.IsTrue(finalBooksList != null && finalBooksList.Count > 0);

			// First book should exist
			Dictionary<object, object> firstBook = (Dictionary<object, object>)finalBooksList[0];
			Assert.IsNotNull(firstBook);

			// Price should exist
			object priceObj = firstBook["price"];
			Assert.IsNotNull(priceObj);

			// Price should be increased (> 11)
			double finalPrice = (double)priceObj;
			Assert.IsTrue(finalPrice > 11.0);

			double expectedPrice = 10.45 * 1.50;
			Assert.IsTrue(Math.Abs(finalPrice - expectedPrice) < 0.01);
		}

		[TestMethod]
		public void TestSelectTitlesWithPriceFilter()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "selectTitlesFilterKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "title", "Cheap Book" },
				{ "price", 5.99 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Medium Book" },
				{ "price", 15.50 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Expensive Book" },
				{ "price", 25.99 }
			};
			booksList.Add(book3);

			Dictionary<string, object> rootMap = [];
			rootMap.Add("book", booksList);

			var bin = new Bin("res1", rootMap);
			client.Put(null, key, bin);

			// Select titles where price <= 10
			CTX ctx1 = CTX.MapKey(Value.Get("book"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(
						MapReturnType.VALUE,
						Exp.Type.FLOAT,
						Exp.Val("price"),
						Exp.MapLoopVar(LoopVarPart.VALUE)
					),
					Exp.Val(10.0)
				)
			);

			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					Exp.StringLoopVar(LoopVarPart.MAP_KEY),
					Exp.Val("title")
				)
			);

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("res1"),
					ctx1, ctx2, ctx3
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("titles", selectExp, ExpWriteFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, key);
			Assert.IsNotNull(finalRecord);

			List<object> titles = (List<object>)finalRecord.GetList("titles");
			// Titles list should exist
			Assert.IsNotNull(titles);
			// Should have 1 book with price <= 10
			Assert.AreEqual(1, titles.Count);
			// First title should be "Cheap Book"
			Assert.AreEqual("Cheap Book", titles[0]);
		}

		[TestMethod]
		public void TestExpReadOpWithSelectByPath()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "expReadOpSelectKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> items =
			[
				10,
				20,
				30
			];
			data.Add("items", items);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select all items
			CTX ctx1 = CTX.MapKey(Value.Get("items"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			// Use ExpReadOp to read without modifying
			Record result = client.Operate(null, key,
				ExpOperation.Read("result", selectExp, ExpReadFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify result
			List<object> resultItems = (List<object>)result.GetList("result");
			// Items should exist
			Assert.IsNotNull(resultItems);
			// Should have 3 items
			Assert.AreEqual(3, resultItems.Count);
		}

		[TestMethod]
		public void TestModifyWithAddition()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "modifyAdditionKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> products = [];

			Dictionary<string, object> p1 = new()
			{
				{ "name", "A" },
				{ "price", 10.00 }
			};
			products.Add(p1);

			Dictionary<string, object> p2 = new()
			{
				{ "name", "B" },
				{ "price", 20.00 }
			};
			products.Add(p2);

			data.Add("products", products);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("products"));
			CTX ctx2 = CTX.AllChildren();
			CTX ctx3 = CTX.MapKey(Value.Get("price"));

			Exp modifyExp = Exp.Add(
				Exp.FloatLoopVar(LoopVarPart.VALUE),
				Exp.Val(5.0)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					ctx1, ctx2, ctx3
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, key);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			Assert.IsNotNull(finalData);

			// Products list should exist
			List<object> finalProducts = (List<object>)finalData["products"];
			Assert.IsNotNull(finalProducts);

			// First product should exist
			Dictionary<object, object> firstProduct = (Dictionary<object, object>)finalProducts[0];
			Assert.IsNotNull(firstProduct);

			object priceObj = firstProduct["price"];
			double priceFloat = (double)priceObj;

			// Verify price is 15.0 (10.0 + 5.0)
			Assert.IsTrue(Math.Abs(priceFloat - 15.0) < 0.01);
		}

		[TestMethod]
		public void TestModifyWithSubtraction()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "modifySubtractionKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> accounts = new()
			{
				{ "acc1", 1000 },
				{ "acc2", 2000 }
			};
			data.Add("accounts", accounts);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Subtract 100 from each account
			CTX ctx1 = CTX.MapKey(Value.Get("accounts"));
			CTX ctx2 = CTX.AllChildren();

			Exp modifyExp = Exp.Sub(
				Exp.IntLoopVar(LoopVarPart.VALUE),
				Exp.Val(100)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify modification
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			Assert.IsNotNull(finalData);

			// Accounts map should exist
			Dictionary<object, object> finalAccounts = (Dictionary<object, object>)finalData["accounts"];
			Assert.IsNotNull(finalAccounts);

			// Account1 should be 900 (1000 - 100)
			long acc1 = (long)finalAccounts["acc1"];
			Assert.AreEqual(900, acc1);
		}

		[TestMethod]
		public void TestExpWriteFlagCreateOnly()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "createOnlyFlagKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values =
			[
				1,
				2,
				3
			];
			data.Add("values", values);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			// This should succeed (new bin)
			Record result = client.Operate(null, key,
				ExpOperation.Write("newbin", selectExp, ExpWriteFlags.CREATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// This should fail (bin already exists)
			try
			{
				client.Operate(null, key,
					ExpOperation.Write("newbin", selectExp, ExpWriteFlags.CREATE_ONLY)
				);
				Assert.Fail("Should have thrown exception for existing bin");
			}
			catch (AerospikeException)
			{
				// Expected
			}
		}

		[TestMethod]
		public void TestCombineSelectAndModify()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "combineSelectModifyKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> items = [];

			Dictionary<string, object> item1 = new()
			{
				{ "id", 1 },
				{ "value", 10 },
			};
			items.Add(item1);

			Dictionary<string, object> item2 = new()
			{
				{ "id", 2 },
				{ "value", 20 },
			};
			items.Add(item2);

			Dictionary<string, object> item3 = new()
			{
				{ "id", 3 },
				{ "value", 30 },
			};
			items.Add(item3);

			data.Add("items", items);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// First, select all values
			CTX selectCtx1 = CTX.MapKey(Value.Get("items"));
			CTX selectCtx2 = CTX.AllChildren();
			CTX selectCtx3 = CTX.MapKey(Value.Get("value"));

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					selectCtx1, selectCtx2, selectCtx3
				)
			);

			// Write selected values to a new bin
			client.Operate(null, key,
				ExpOperation.Write("values", selectExp, ExpWriteFlags.DEFAULT)
			);

			// Then, modify the values by doubling them
			CTX modifyCtx1 = CTX.MapKey(Value.Get("items"));
			CTX modifyCtx2 = CTX.AllChildren();
			CTX modifyCtx3 = CTX.MapKey(Value.Get("value"));

			Exp modifyExp = Exp.Mul(
				Exp.IntLoopVar(LoopVarPart.VALUE),
				Exp.Val(2)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					modifyCtx1, modifyCtx2, modifyCtx3
				)
			);

			client.Operate(null, key,
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Verify both bins
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Check original values (should be [10, 20, 30])
			List<object> values = (List<object>)finalRecord.GetList("values");
			// Values should exist
			Assert.IsNotNull(values);
			// Should have 3 values
			Assert.AreEqual(3, values.Count);

			// Check modified data (values should be doubled)
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			// Data map should exist
			Assert.IsNotNull(finalData);

			// Items list should exist
			List<object> finalItems = (List<object>)finalData["items"];
			Assert.IsNotNull(finalItems);

			// First item should exist
			Dictionary<object, object> firstItem = (Dictionary<object, object>)finalItems[0];
			Assert.IsNotNull(firstItem);

			// Value should be doubled 20 (10 * 2)
			long value = (long)firstItem["value"];
			Assert.AreEqual(20, value);
		}

		[TestMethod]
		public void TestSelectByPathWithListOfLists()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "listOfListsKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<List<int>> matrix = [];

			List<int> row1 = [1, 2, 3];
			matrix.Add(row1);

			List<int> row2 = [4, 5, 6];
			matrix.Add(row2);

			List<int> row3 = [7, 8, 9];
			matrix.Add(row3);

			data.Add("matrix", matrix);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select all rows
			CTX ctx1 = CTX.MapKey(Value.Get("matrix"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("rows", selectExp, ExpWriteFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify result
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			List<object> rows = (List<object>)finalRecord.GetList("rows");
			// Rows should exist
			Assert.IsNotNull(rows);
			// Should have 3 rows
			Assert.AreEqual(3, rows.Count);
		}

		[TestMethod]
		public void TestModifyNestedMapValues()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "modifyNestedMapKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> departments = [];

			Dictionary<string, object> sales = new()
			{
				{ "revenue", 100000 },
				{ "target", 120000 },
			};
			departments.Add("sales", sales);

			Dictionary<string, object> engineering = new()
			{
				{ "revenue", 50000 },
				{ "target", 60000 },
			};
			departments.Add("engineering", engineering);

			data.Add("departments", departments);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Increase all revenue by 10%
			CTX ctx1 = CTX.MapKey(Value.Get("departments"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					Exp.StringLoopVar(LoopVarPart.MAP_KEY),
					Exp.Val("revenue")
				)
			);

			Exp modifyExp = Exp.Mul(
				Exp.IntLoopVar(LoopVarPart.VALUE),
				Exp.Val(2)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					ctx1, ctx2, ctx3
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify modification
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			Assert.IsNotNull(finalData);

			// Departments map should exist
			Dictionary<object, object> depts = (Dictionary<object, object>)finalData["departments"];
			Assert.IsNotNull(depts);

			// Sales department should exist
			Dictionary<object, object> salesDept = (Dictionary<object, object>)depts["sales"];
			Assert.IsNotNull(salesDept);

			object revenue = salesDept["revenue"];
			long revenueLong = (long)revenue;

			long expectedRevenue = 100000 * 2;
			Assert.AreEqual(expectedRevenue, revenueLong);
		}

		[TestMethod]
		public void TestSelectByPathWithIntegerValues()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "selectIntegerValuesKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> scores = new()
			{
				{ "player1", 100 },
				{ "player2", 200 },
				{ "player3", 150 }
			};
			data.Add("scores", scores);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select all scores
			CTX ctx1 = CTX.MapKey(Value.Get("scores"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Read("allScores", selectExp, ExpReadFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify result
			List<object> scoresList = (List<object>)result.GetList("allScores");
			// Scores should exist
			Assert.IsNotNull(scoresList);
			// Should have 3 scores
			Assert.AreEqual(3, scoresList.Count);
		}

		[TestMethod]
		public void TestModifyWithDivision()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "modifyDivisionKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values =
			[
				100,
				200,
				300
			];
			data.Add("values", values);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Divide all values by 10
			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildren();

			Exp modifyExp = Exp.Div(
				Exp.IntLoopVar(LoopVarPart.VALUE),
				Exp.Val(10)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify modification
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			Assert.IsNotNull(finalData);

			// Values list should exist
			List<object> finalValues = (List<object>)finalData["values"];
			Assert.IsNotNull(finalValues);

			long firstValue = (long)finalValues[0];
			Assert.AreEqual(10, firstValue);
		}

		[TestMethod]
		public void TestSelectByPathWithMapKeys()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "selectMapKeysKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> products = new()
			{
				{ "apple", 1.50 },
				{ "banana", 0.75 },
				{ "cherry", 2.25 }
			};
			data.Add("products", products);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select all keys
			CTX ctx1 = CTX.MapKey(Value.Get("products"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.MAP_KEY,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("keys", selectExp, ExpWriteFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify result - should get keys
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Keys list should exist
			List<object> keys = (List<object>)finalRecord.GetList("keys");
			Assert.IsNotNull(keys);
		}

		[TestMethod]
		public void TestSelectByPathWithFilteredResults()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "selectFilteredKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> employees = [];

			Dictionary<string, object> emp1 = new()
			{
				{ "name", "Alice" },
				{ "salary", 50000 },
				{ "active", true }
			};
			employees.Add(emp1);

			Dictionary<string, object> emp2 = new()
			{
				{ "name", "Bob" },
				{ "salary", 60000 },
				{ "active", false }
			};
			employees.Add(emp2);

			Dictionary<string, object> emp3 = new()
			{
				{ "name", "Charlie" },
				{ "salary", 55000 },
				{ "active", true }
			};
			employees.Add(emp3);

			data.Add("employees", employees);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select names of active employees
			CTX ctx1 = CTX.MapKey(Value.Get("employees"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					MapExp.GetByKey(
						MapReturnType.VALUE,
						Exp.Type.BOOL,
						Exp.Val("active"),
						Exp.MapLoopVar(LoopVarPart.VALUE)
					),
					Exp.Val(true)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					Exp.StringLoopVar(LoopVarPart.MAP_KEY),
					Exp.Val("name")
				)
			);

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2, ctx3
				)
			);

			Record result = client.Operate(null, key,
				ExpOperation.Write("activeEmployees", selectExp, ExpWriteFlags.DEFAULT)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify result
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			List<object> names = (List<object>)finalRecord.GetList("activeEmployees");
			// Names should exist
			Assert.IsNotNull(names);
			// Should have 2 active employees
			Assert.AreEqual(2, names.Count);
			// Should contain "Alice"
			Assert.IsTrue(names.Contains("Alice"));
			// Should contain "Charlie"
			Assert.IsTrue(names.Contains("Charlie"));
		}

		[TestMethod]
		public void TestExpWriteFlagEvalNoFail()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "evalNoFailKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			// Don't create the bin
			var bin = new Bin("otherbin", "test");
			client.Put(null, key, bin);

			// Try to select from non-existent bin with EvalNoFail
			CTX ctx1 = CTX.MapKey(Value.Get("items"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("nonexistent"),
					ctx1, ctx2
				)
			);

			// Should not fail with EVAL_NO_FAIL flag
			Record result = client.Operate(null, key,
				ExpOperation.Write("result", selectExp, ExpWriteFlags.EVAL_NO_FAIL)
			);

			// Operation should succeed with EVAL_NO_FAIL
			Assert.IsNotNull(result);
		}

		[TestMethod]
		public void TestMultipleExpWriteOpInSequence()
		{
			var key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "multipleOpsKey");

			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values =
			[
				1,
				2,
				3
			];
			data.Add("values", values);

			var bin = new Bin("data", data);
			client.Put(null, key, bin);

			// Select values
			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildren();

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			// Modify values (double them)
			Exp modifyExp = Exp.Mul(
				Exp.IntLoopVar(LoopVarPart.VALUE),
				Exp.Val(2)
			);

			Expression applyExp = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					modifyExp,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			// Execute both operations in one call
			Record result = client.Operate(null, key,
				ExpOperation.Write("original", selectExp, ExpWriteFlags.DEFAULT),
				ExpOperation.Write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
			);

			// Operation should succeed
			Assert.IsNotNull(result);

			// Verify both results
			Record finalRecord = client.Get(null, key);
			// Final record should exist
			Assert.IsNotNull(finalRecord);

			// Original values should be [1, 2, 3]
			List<object> original = (List<object>)finalRecord.GetList("original");
			// Original values should exist
			Assert.IsNotNull(original);
			// Should have 3 original values
			Assert.AreEqual(3, original.Count);

			// Modified values should be doubled
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue("data");
			// Data map should exist
			Assert.IsNotNull(finalData);

			// Values list should exist
			List<object> finalValues = (List<object>)finalData["values"];
			Assert.IsNotNull(finalValues);

			long firstValue = (long)finalValues[0];
			Assert.AreEqual(2, firstValue);
		}
	}
}
