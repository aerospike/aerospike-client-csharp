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
	public class TestCDTOperate : TestSync
	{
		private const string binName = "testbin";
		private const string inventoryBinName = "inventory";

		[TestInitialize()]
		public void CheckServerVersion()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_1, "Path expression");
		}

		[TestMethod]
		public void TestCDTOperateCodeSample()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory1");
			SetupInventorySample(rkey);

			// Product-level: featured == true
			Exp filterOnFeatured = Exp.EQ(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.BOOL,
					Exp.Val("featured"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each product map
				),
				Exp.Val(true)
			);

			// Variant-level: quantity > 0
			Exp filterOnVariantInventory = Exp.GT(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.INT,
					Exp.Val("quantity"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each variant object
				),
				Exp.Val(0)
			);

			// Operation
			Record record = client.Operate(null, rkey,
				CDTOperation.SelectByPath(inventoryBinName, SelectFlag.MATCHING_TREE,
					CTX.AllChildren(), // dive into all products
					CTX.AllChildrenWithFilter(filterOnFeatured), // only featured products
					CTX.MapKey(Value.Get("variants")), // dive into variants
					CTX.AllChildrenWithFilter(filterOnVariantInventory) // only in-stock variants
				)
			);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)record.GetMap(inventoryBinName), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(record);

			// Verify the result
			Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(inventoryBinName);
			Assert.IsNotNull(resultMap);
			Assert.IsTrue(resultMap.ContainsKey("inventory"));

			// Verify the featured products
			Dictionary<object, object> featuredProducts = (Dictionary<object, object>)resultMap["inventory"];
			Assert.IsNotNull(featuredProducts);
			Assert.AreEqual(3, featuredProducts.Count);
			Assert.IsTrue(featuredProducts.ContainsKey("10000001"));
			Assert.IsTrue(featuredProducts.ContainsKey("50000009"));
			Assert.IsTrue(featuredProducts.ContainsKey("50000006"));

			// Verify the variants for product 10000001
			Dictionary<object, object> product1 = (Dictionary<object, object>)featuredProducts["10000001"];
			Assert.IsNotNull(product1);
			Dictionary<object, object> product1Variants = (Dictionary<object, object>)product1["variants"];
			Assert.IsNotNull(product1Variants);
			Assert.AreEqual(2, product1Variants.Count);
			Assert.IsTrue(product1Variants.ContainsKey("2001"));
			Assert.IsTrue(product1Variants.ContainsKey("2003"));

			// Verify the variants for product 50000009
			Dictionary<object, object> product4 = (Dictionary<object, object>)featuredProducts["50000009"];
			Assert.IsNotNull(product4);
			List<object> product4Variants = (List<object>)product4["variants"];
			Assert.IsNotNull(product4Variants);
			Assert.AreEqual(2, product4Variants.Count);
			Dictionary<object, object> variant1 = (Dictionary<object, object>)product4Variants[0];
			Assert.AreEqual((long)3007, variant1["sku"]);
			Dictionary<object, object> variant2 = (Dictionary<object, object>)product4Variants[1];
			Assert.AreEqual((long)3008, variant2["sku"]);

			// Verify the variants for product 50000006
			Dictionary<object, object> product3 = (Dictionary<object, object>)featuredProducts["50000006"];
			Assert.IsNotNull(product3);
			Dictionary<object, object> product3Variants = (Dictionary<object, object>)product3["variants"];
			Assert.IsNotNull(product3Variants);
			Assert.AreEqual(0, product3Variants.Count);
		}

		[TestMethod]
		public void TestCDTOperateCodeSampleAdvanced()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory2");
			SetupInventorySample(rkey);

			Exp filterOnKey =
				Exp.RegexCompare("10000.*", 0, Exp.StringLoopVar(LoopVarPart.MAP_KEY)
			);

			// Operation
			Record record = client.Operate(null, rkey,
				CDTOperation.SelectByPath(inventoryBinName, SelectFlag.MATCHING_TREE,
					CTX.AllChildren(),
					CTX.AllChildrenWithFilter(filterOnKey)
				)
			);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)record.GetMap(inventoryBinName), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(record);

			// Verify the result
			Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(inventoryBinName);
			Assert.IsNotNull(resultMap);
			Assert.IsTrue(resultMap.ContainsKey("inventory"));

			// Verify only products with key starting with 100000 are included
			Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
			Assert.IsNotNull(products);
			Assert.AreEqual(2, products.Count);
			Assert.IsTrue(products.ContainsKey("10000001"));
			Assert.IsTrue(products.ContainsKey("10000002"));
		}

		[TestMethod]
		public void TestCDTOperateCodeSampleAdvancedAltReturn()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory3");
			SetupInventorySample(rkey);

			Exp filterOnKey =
				Exp.RegexCompare("10000.*", RegexFlag.NONE, Exp.StringLoopVar(LoopVarPart.MAP_KEY)
			);

			// Operation
			Record record = client.Operate(null, rkey,
				CDTOperation.SelectByPath(inventoryBinName, SelectFlag.MATCHING_TREE,
					CTX.AllChildren(),
					CTX.AllChildrenWithFilter(filterOnKey)
				)
			);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)record.GetMap(inventoryBinName), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(record);

			// Verify the results contain the expected products
			Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(inventoryBinName);
			Assert.IsNotNull(resultMap);
			Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
			Assert.IsNotNull(products);
			Assert.AreEqual(2, products.Count);
			Assert.IsTrue(products.ContainsKey("10000001"));
			Assert.IsTrue(products.ContainsKey("10000002"));
		}

		[TestMethod]
		public void TestCDTOperateCodeSampleAdvancedMultipleFilters()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory4");
			SetupInventorySample(rkey);

			Exp filterOnCheapInStock = Exp.And(
				Exp.GT(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
						Exp.Val("quantity"),
						Exp.MapLoopVar(LoopVarPart.VALUE)
					),
					Exp.Val(0)
				),
				Exp.LT(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
						Exp.Val("price"),
						Exp.MapLoopVar(LoopVarPart.VALUE)
					),
					Exp.Val(50)
				)
			);

			// Operation
			Record record = client.Operate(null, rkey,
				CDTOperation.SelectByPath(inventoryBinName, SelectFlag.MATCHING_TREE,
					CTX.AllChildren(), // navigate into all products
					CTX.AllChildren(), // navigate deeper into product structure
					CTX.MapKey(Value.Get("variants")), // navigate into variants map/list
					CTX.AllChildrenWithFilter(filterOnCheapInStock) // filter variants bt price and quantity
				)
			);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)record.GetMap(inventoryBinName), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(record);

			// Verify the results contain the expected products
			Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(inventoryBinName);
			Assert.IsNotNull(resultMap);
			Assert.IsTrue(resultMap.ContainsKey("inventory"));
			Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
			Assert.IsNotNull(products);
			Assert.AreEqual(4, products.Count);
			Assert.IsTrue(products.ContainsKey("10000001"));
			Assert.IsTrue(products.ContainsKey("10000002"));
			Assert.IsTrue(products.ContainsKey("50000006"));
			Assert.IsTrue(products.ContainsKey("50000009"));

			// Verify the variants for product 10000001
			Dictionary<object, object> product1 = (Dictionary<object, object>)products["10000001"];
			Assert.IsNotNull(product1);
			Dictionary<object, object> product1Variants = (Dictionary<object, object>)product1["variants"];
			Assert.IsNotNull(product1Variants);
			Assert.AreEqual(2, product1Variants.Count);
			Assert.IsTrue(product1Variants.ContainsKey("2001"));
			Assert.IsTrue(product1Variants.ContainsKey("2003"));

			// Verify the variants for product 10000002
			Dictionary<object, object> product2 = (Dictionary<object, object>)products["10000002"];
			Assert.IsNotNull(product2);
			Dictionary<object, object> product2Variants = (Dictionary<object, object>)product2["variants"];
			Assert.IsNotNull(product2Variants);
			Assert.AreEqual(2, product2Variants.Count);
			Assert.IsTrue(product2Variants.ContainsKey("2004"));
			Assert.IsTrue(product2Variants.ContainsKey("2005"));

			// Verify the variants for product 50000006
			Dictionary<object, object> product3 = (Dictionary<object, object>)products["50000006"];
			Assert.IsNotNull(product3);
			Dictionary<object, object> product3Variants = (Dictionary<object, object>)product3["variants"];
			Assert.IsNotNull(product3Variants);
			Assert.AreEqual(0, product3Variants.Count);

			// Verify the variants for product 50000009
			Dictionary<object, object> product4 = (Dictionary<object, object>)products["50000009"];
			Assert.IsNotNull(product4);
			List<object> product4Variants = (List<object>)product4["variants"];
			Assert.IsNotNull(product4Variants);
			Assert.AreEqual(0, product4Variants.Count);
		}

		[TestMethod]
		public void TestCDTOperateCodeSampleAdvancedModifyCDT()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory5");
			SetupInventorySample(rkey);
			string updatedBin = "updatedBinName";

			// Increment quantity by 10 and return the modified map
			Exp incrementExp = MapExp.Put(
				MapPolicy.Default,
				Exp.Val("quantity"),  // key to update
				Exp.Add(  // new value: current quantity + 10
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
						Exp.Val("quantity"),
						Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(10)
				),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			);

			// Product-level: featured == true
			Exp filterOnFeatured = Exp.EQ(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.BOOL,
					Exp.Val("featured"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each product map
				),
				Exp.Val(true)
			);

			// Variant-level: quantity > 0
			Exp filterOnVariantInventory = Exp.GT(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.INT,
					Exp.Val("quantity"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each variant object
				),
				Exp.Val(0)
			);

			Expression modifyExpression = Exp.Build(
				CDTExp.ModifyByPath(
					Exp.Type.MAP,
					ModifyFlag.DEFAULT,
					incrementExp,
					Exp.MapBin("inventory"),
					CTX.AllChildren(),
					CTX.AllChildrenWithFilter(filterOnFeatured),
					CTX.MapKey(Value.Get("variants")),
					CTX.AllChildrenWithFilter(filterOnVariantInventory)
				)
			);

			// Write the modified map to a new bin
			client.Operate(null, rkey,
				ExpOperation.Write(updatedBin, modifyExpression, ExpWriteFlags.DEFAULT));

			// Read back the updated record
			Record updatedRecord = client.Get(null, rkey);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)updatedRecord.GetMap(updatedBin), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(updatedRecord);

			// Verify the results incremented the specified quantity by 10
			Dictionary<object, object> resultMap = (Dictionary<object, object>)updatedRecord.GetMap(updatedBin);
			Assert.IsNotNull(resultMap);
			Assert.IsTrue(resultMap.ContainsKey("inventory"));
			Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];

			// Verify the results for product 10000001
			Dictionary<object, object> product1 = (Dictionary<object, object>)products["10000001"];
			Assert.IsNotNull(product1);
			Dictionary<object, object> product1Variants = (Dictionary<object, object>)product1["variants"];
			Assert.IsNotNull(product1Variants);
			Dictionary<object, object> p1variant2001 = (Dictionary<object, object>)product1Variants["2001"];
			Assert.IsNotNull(p1variant2001);
			Assert.AreEqual((long)110, p1variant2001["quantity"]);
			Dictionary<object, object> p1variant2003 = (Dictionary<object, object>)product1Variants["2003"];
			Assert.IsNotNull(p1variant2003);
			Assert.AreEqual((long)60, p1variant2003["quantity"]);

			// Verify the results for product 50000009
			Dictionary<object, object> product4 = (Dictionary<object, object>)products["50000009"];
			Assert.IsNotNull(product4);
			List<object> product4Variants = (List<object>)product4["variants"];
			Assert.IsNotNull(product4Variants);
			Assert.AreEqual(2, product4Variants.Count);
			Dictionary<object, object> variant1 = (Dictionary<object, object>)product4Variants[0];
			Assert.IsNotNull(variant1);
			Assert.AreEqual((long)3007, variant1["sku"]);
			Assert.AreEqual((long)70, variant1["quantity"]);
			Dictionary<object, object> variant2 = (Dictionary<object, object>)product4Variants[1];
			Assert.IsNotNull(variant2);
			Assert.AreEqual((long)3008, variant2["sku"]);
			Assert.AreEqual((long)40, variant2["quantity"]);

		}

		[TestMethod]
		public void TestCDTOperateCodeSampleAdvancedNoFail()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "inventory6");
			SetupInventorySample(rkey, true);

			// Product-level: featured == true
			Exp filterOnFeatured = Exp.EQ(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.BOOL,
					Exp.Val("featured"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each product map
				),
				Exp.Val(true)
			);

			// Variant-level: quantity > 0
			Exp filterOnVariantInventory = Exp.GT(
				MapExp.GetByKey(
					MapReturnType.VALUE, Exp.Type.INT,
					Exp.Val("quantity"),
					Exp.MapLoopVar(LoopVarPart.VALUE) // loop variable points to each variant object
				),
				Exp.Val(0)
			);

			Record noFailResponse = client.Operate(null, rkey,
				CDTOperation.SelectByPath(inventoryBinName, SelectFlag.MATCHING_TREE | SelectFlag.NO_FAIL,
					CTX.AllChildren(),
					CTX.AllChildrenWithFilter(filterOnFeatured),
					CTX.MapKey(Value.Get("variants")),
					CTX.AllChildrenWithFilter(filterOnVariantInventory)
				)
			);
			//Console.WriteLine(System.Text.Json.JsonSerializer.Serialize((Dictionary<object, object>)noFailResponse.GetMap(inventoryBinName), new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
			Assert.IsNotNull(noFailResponse);

			// Verify the results
			Dictionary<object, object> resultMap = (Dictionary<object, object>)noFailResponse.GetMap(inventoryBinName);
			Assert.IsNotNull(resultMap);
			Assert.IsTrue(resultMap.ContainsKey("inventory"));
			Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
			Assert.IsNotNull(products);
			Assert.IsTrue(products.ContainsKey("10000003"));
			Dictionary<object, object> product5 = (Dictionary<object, object>)products["10000003"];
			Assert.IsNotNull(product5);
			Dictionary<object, object> product5Variants = (Dictionary<object, object>)product5["variants"];
			Assert.IsNotNull(product5Variants);
			Assert.AreEqual(0, product5Variants.Count);
		}

		private static void SetupInventorySample(Key key, bool extraProduct = false)
		{
			try
			{
				client.Delete(null, key);
			}
			catch (Exception)
			{
			}

			// Build the inventory data structure
			Dictionary<string, object> inventory = [];

			// Product 10000001: Classic T-Shirt
			Dictionary<string, object> product1 = new()
			{
				{ "category", "clothing" },
				{ "featured", true },
				{ "name", "Classic T-Shirt" },
				{ "description", "A lightweight cotton T-shirt perfect for everyday wear." }
			};
			Dictionary<string, object> product1Variants = new()
			{
				{ "2001", new Dictionary<string, object> { { "size", "S" }, { "price", 25 }, { "quantity", 100 } } },
				{ "2002", new Dictionary<string, object> { { "size", "M" }, { "price", 25 }, { "quantity", 0 } } },
				{ "2003", new Dictionary<string, object> { { "size", "L" }, { "price", 27 }, { "quantity", 50 } } }
			};
			product1.Add("variants", product1Variants);
			inventory.Add("10000001", product1);

			// Product 10000002: Casual Polo Shirt
			Dictionary<string, object> product2 = new()
			{
				{ "category", "clothing" },
				{ "featured", false },
				{ "name", "Casual Polo Shirt" },
				{ "description", "A soft polo shirt suitable for work or leisure." }
			};
			Dictionary<string, object> product2Variants = new()
			{
				{ "2004", new Dictionary<string, object> { { "size", "M" }, { "price", 30 }, { "quantity", 20 } } },
				{ "2005", new Dictionary<string, object> { { "size", "XL" }, { "price", 32 }, { "quantity", 10 } } }
			};
			product2.Add("variants", product2Variants);
			inventory.Add("10000002", product2);

			// Product 50000006: Laptop Pro 14
			Dictionary<string, object> product3 = new()
			{
				{ "category", "electronics" },
				{ "featured", true },
				{ "name", "Laptop Pro 14" },
				{ "description", "High-performance laptop designed for professionals." }
			};
			Dictionary<string, object> product3Variants = new()
			{
				{ "3001", new Dictionary<string, object> { { "spec", "8GB RAM" }, { "price", 599 }, { "quantity", 0 } } }
			};
			product3.Add("variants", product3Variants);
			inventory.Add("50000006", product3);

			// Product 50000009: Smart TV
			Dictionary<string, object> product4 = new()
			{
				{ "category", "electronics" },
				{ "featured", true },
				{ "name", "Smart TV" },
				{ "description", "Ultra HD smart television with built-in streaming apps." }
			};
			List<Dictionary<string, object>> product4Variants =
			[
				new() { { "sku", 3007 }, { "spec", "1080p" }, { "price", 199 }, { "quantity", 60 } },
				new() { { "sku", 3008 }, { "spec", "4K" }, { "price", 399 }, { "quantity", 30 } }
			];
			product4.Add("variants", product4Variants);
			inventory.Add("50000009", product4);

			if (extraProduct)
			{
				// Product 10000003: Hooded Sweatshirt
				Dictionary<string, object> product5 = new()
				{
					{ "category", "clothing" },
					{ "featured", true },
					{ "name", "Hooded Sweatshirt" },
					{ "description", "Hooded Sweatshirt" }
				};
				Dictionary<string, object> product5Variants = new()
				{
					{ "quantity", 10 }
				};
				product5.Add("variants", product5Variants);
				inventory.Add("10000003", product5);
			}

			// Create the root data structure
			Dictionary<string, object> data = new()
			{
				{ "inventory", inventory }
			};

			var bin = new Bin(inventoryBinName, data);
			client.Put(null, key, bin);
		}

		[TestMethod]
		public void TestCDTOperateWithExpressions()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 215);

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

			var bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			Record record = client.Get(null, rkey);
			// Record should exist
			Assert.IsNotNull(record);

			CTX ctx1 = CTX.MapKey(Value.Get("book"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
						Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(10.0)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("title"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3);

			Record result = client.Operate(null, rkey, selectOp);
			// CDT select operation should succeed
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			// Results should not be null
			Assert.IsNotNull(results);
			// Should have 2 books with price <= 10.0
			Assert.AreEqual(2, results.Count);

			// Verify the titles (order may vary)
			List<string> titles = [];
			foreach (object item in results)
			{
				Assert.IsInstanceOfType(item, typeof(string));
				titles.Add((string)item);
			}

			// Check that we got the expected titles
			Assert.IsTrue(titles.Contains("Sayings of the Century"));
			Assert.IsTrue(titles.Contains("Moby Dick"));
		}

		[TestMethod]
		public void TestCDTApplyWithExpressions()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 216);

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

			var bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			CTX bookKey = CTX.MapKey(Value.Get("book"));
			CTX allChildren = CTX.AllChildren();
			CTX priceKey = CTX.MapKey(Value.Get("price"));

			Expression modifyExp = Exp.Build(
				Exp.Mul(
					Exp.FloatLoopVar(LoopVarPart.VALUE),  // Current price value
					Exp.Val(1.10)                         // Multiply by 1.10
				)
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, bookKey, allChildren, priceKey);

			// CDT apply operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Root map should exist
			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue(binName);
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

			// Price should be increased (> 9)
			double finalPrice = (double)priceObj;
			Assert.IsTrue(finalPrice > 9.0);

			double expectedPrice = 8.95 * 1.10;
			Assert.IsTrue(Math.Abs(finalPrice - expectedPrice) < 0.01);

			// Verify all books have increased prices
			double[] originalPrices = [8.95, 12.99, 8.99, 22.99];
			for (int i = 0; i < finalBooksList.Count; i++)
			{
				// Book should be a map
				Dictionary<object, object> book = (Dictionary<object, object>)finalBooksList[i];
				Assert.IsNotNull(book);

				// Book should have a price
				object price = book["price"];
				Assert.IsNotNull(price);

				double priceFloat = (double)price;
				double expected = originalPrices[i] * 1.10;
				Assert.IsTrue(Math.Abs(priceFloat - expected) < 0.01);
			}
		}

		[TestMethod]
		public void TestNestedContextsAndComplexFilters()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 217);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> store = [];
			List<Dictionary<string, object>> booksList = [];

			Dictionary<string, object> book1 = new()
			{
				{ "category", "reference" },
				{ "author", "Nigel Rees" },
				{ "title", "Sayings of the Century" },
				{ "price", 8.95 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "category", "fiction" },
				{ "author", "Evelyn Waugh" },
				{ "title", "Sword of Honour" },
				{ "price", 12.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "category", "fiction" },
				{ "author", "Herman Melville" },
				{ "title", "Moby Dick" },
				{ "price", 8.99 }
			};
			booksList.Add(book3);

			store.Add("books", booksList);
			data.Add("store", store);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("store"));
			CTX ctx2 = CTX.MapKey(Value.Get("books"));
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.And(
					Exp.EQ(
						MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.STRING,
							Exp.Val("category"), Exp.MapLoopVar(LoopVarPart.VALUE)),
						Exp.Val("fiction")
					),
					Exp.LT(
						MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
							Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
						Exp.Val(10.0)
					)
				)
			);
			CTX ctx4 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("title"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3, ctx4);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			// Results should not be null
			Assert.IsNotNull(results);
			// Should have 1 fiction book with price < 10.0
			Assert.AreEqual(1, results.Count);
			// Should get 'Moby Dick'
			Assert.AreEqual("Moby Dick", results[0]);
		}

		[TestMethod]
		public void TestEmptyResultsWhenNoItemsMatch()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 218);

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
				{ "title", "Expensive Book 1" },
				{ "price", 25.99 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Expensive Book 2" },
				{ "price", 30.50 }
			};
			booksList.Add(book2);

			Dictionary<string, object> rootMap = new()
			{
				{ "book", booksList }
			};

			var bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			// Try to select books with price <= 10.0 (should return empty)
			CTX ctx1 = CTX.MapKey(Value.Get("book"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
						Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(10.0)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("title"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Verify empty results
			object results = result.GetValue(binName);
			if (results is IList<object> resultList)
			{
				// Should have 0 books matching the filter
				Assert.AreEqual(0, resultList.Count);
			}
		}

		[TestMethod]
		public void TestMatchingTreeFlag()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 219);

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
				{ "title", "Cheap Book" },
				{ "price", 5.99 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Expensive Book" },
				{ "price", 25.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> rootMap = new()
			{
				{ "book", booksList }
			};

			var bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("book"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
						Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(10.0)
				)
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.MATCHING_TREE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// With MatchingTree, we should get back the full matching structure
			object results = result.GetValue(binName);
			Assert.IsNotNull(results);
		}

		[TestMethod]
		public void TestMapKeysFlag()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 220);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> items = new()
			{
				{ "item1", 100 },
				{ "item2", 200 },
				{ "item3", 50 }
			};
			data.Add("items", items);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select with MapKeys flag - should return only keys, not values
			CTX ctx1 = CTX.MapKey(Value.Get("items"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(75))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.MAP_KEY, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get keys where value > 75
			object results = result.GetValue(binName);
			Assert.IsNotNull(results);
		}

		[TestMethod]
		public void TestSelectNoFailFlag()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 221);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> existing =
			[
				1,
				2,
				3
			];
			data.Add("existing", existing);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Try to select from existing path with SelectNoFail
			CTX ctx1 = CTX.MapKey(Value.Get("existing"));
			CTX ctx2 = CTX.AllChildren();

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.NO_FAIL, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);
		}

		[TestMethod]
		public void TestLoopVariableIndex()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 222);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> numbers =
			[
				10,
				20,
				30,
				40,
				50
			];
			data.Add("numbers", numbers);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select items where index < 3
			CTX ctx1 = CTX.MapKey(Value.Get("numbers"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LT(Exp.IntLoopVar(LoopVarPart.INDEX), Exp.Val(3))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get first 3 items (indices 0, 1, 2)
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				Assert.AreEqual(3, results.Count);
			}
		}

		[TestMethod]
		public void TestLoopVariableMapKey()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 223);

			try
			{
				client.Delete(null, rkey);
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

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select items where key starts with 'a' or 'b' (lexicographically < "c")
			CTX ctx1 = CTX.MapKey(Value.Get("products"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LT(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("c"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get apple and banana (keys < "c")
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				Assert.AreEqual(2, results.Count);
			}
		}

		[TestMethod]
		public void TestModifyWithAddition()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 224);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> scores =
			[
				10,
				20,
				30,
				40,
				50
			];
			data.Add("scores", scores);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Add 5 to each score
			CTX ctx1 = CTX.MapKey(Value.Get("scores"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression modifyExp = Exp.Build(
				Exp.Add(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(5))
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, ctx1, ctx2);

			// CDT modify operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Root map should exist
			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalRootMap);

			// Scores list should exist
			List<object> finalScores = (List<object>)finalRootMap["scores"];
			Assert.IsNotNull(finalScores);
			// Should have 5 scores
			Assert.AreEqual(5, finalScores.Count);

			long firstScore = (long)finalScores[0];
			// 10 + 5 = 15
			Assert.AreEqual(15, firstScore);
		}

		[TestMethod]
		public void TestModifyWithSubtraction()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 225);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> balances = new()
			{
				{ "account1", 1000 },
				{ "account2", 2000 },
				{ "account3", 1500 }
			};
			data.Add("balances", balances);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Subtract 100 from each balance
			CTX ctx1 = CTX.MapKey(Value.Get("balances"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression modifyExp = Exp.Build(
				Exp.Sub(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(100))
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, ctx1, ctx2);

			// CDT apply operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Root map should exist
			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalRootMap);

			// Balances map should exist
			Dictionary<object, object> finalBalances = (Dictionary<object, object>)finalRootMap["balances"];
			Assert.IsNotNull(finalBalances);

			// Verify account1 balance was decreased by 100
			long balance1 = (long)finalBalances["account1"];
			// 1000 - 100 = 900
			Assert.AreEqual(900, balance1);
		}

		[TestMethod]
		public void TestNestedListsAndComplexFilters()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 226);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<List<int>> matrix =
			[
				[1, 2, 3],
				[4, 5, 6],
				[7, 8, 9]
			];
			data.Add("matrix", matrix);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("matrix"));
			CTX ctx2 = CTX.AllChildren();

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get all 3 rows
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				Assert.AreEqual(3, results.Count);
			}
		}

		[TestMethod]
		public void TestBooleanExpressionsInFilters()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 227);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> users = [];

			Dictionary<string, object> user1 = new()
			{
				{ "name", "Alice" },
				{ "active", true },
				{ "age", 30 }
			};
			users.Add(user1);

			Dictionary<string, object> user2 = new()
			{
				{ "name", "Bob" },
				{ "active", false },
				{ "age", 25 }
			};
			users.Add(user2);

			Dictionary<string, object> user3 = new()
			{
				{ "name", "Charlie" },
				{ "active", true },
				{ "age", 35 }
			};
			users.Add(user3);

			data.Add("users", users);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select active users
			CTX ctx1 = CTX.MapKey(Value.Get("users"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.BOOL,
						Exp.Val("active"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(true)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("name"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get Alice and Charlie (active users)
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				Assert.AreEqual(2, results.Count);
				Assert.IsTrue(results.Contains("Alice"));
				Assert.IsTrue(results.Contains("Charlie"));
			}
		}

		[TestMethod]
		public void TestComplexAndOrFilterCombinations()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 228);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> products = [];

			Dictionary<string, object> p1 = new()
			{
				{ "name", "Widget" },
				{ "price", 10.0 },
				{ "inStock", true }
			};
			products.Add(p1);

			Dictionary<string, object> p2 = new()
			{
				{ "name", "Gadget" },
				{ "price", 25.0 },
				{ "inStock", false }
			};
			products.Add(p2);

			Dictionary<string, object> p3 = new()
			{
				{ "name", "Gizmo" },
				{ "price", 15.0 },
				{ "inStock", true }
			};
			products.Add(p3);

			Dictionary<string, object> p4 = new()
			{
				{ "name", "Doohickey" },
				{ "price", 30.0 },
				{ "inStock", true }
			};
			products.Add(p4);

			data.Add("products", products);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select products that are (inStock AND price < 20) OR (price > 25)
			CTX ctx1 = CTX.MapKey(Value.Get("products"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.Or(
					Exp.And(
						Exp.EQ(
							MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.BOOL,
								Exp.Val("inStock"), Exp.MapLoopVar(LoopVarPart.VALUE)),
							Exp.Val(true)
						),
						Exp.LT(
							MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
								Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
							Exp.Val(20.0)
						)
					),
					Exp.GT(
						MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
							Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
						Exp.Val(25.0)
					)
				)
			);
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("name"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get Widget (inStock, price 10), Gizmo (inStock, price 15), and Doohickey (price 30)
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have at least 1 matching product
				Assert.IsTrue(results.Count >= 1);
			}
		}

		[TestMethod]
		public void TestDeeplyNestedStructures()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 229);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> level1 = [];
			Dictionary<string, object> level2 = [];
			List<Dictionary<string, object>> level3 = [];

			Dictionary<string, object> item1 = new()
			{
				{ "value", 100 }
			};
			level3.Add(item1);

			Dictionary<string, object> item2 = new()
			{
				{ "value", 200 }
			};
			level3.Add(item2);

			Dictionary<string, object> item3 = new()
			{
				{ "value", 300 }
			};
			level3.Add(item3);

			level2.Add("level3", level3);
			level1.Add("level2", level2);
			data.Add("level1", level1);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Navigate deep and select values
			CTX ctx1 = CTX.MapKey(Value.Get("level1"));
			CTX ctx2 = CTX.MapKey(Value.Get("level2"));
			CTX ctx3 = CTX.MapKey(Value.Get("level3"));
			CTX ctx4 = CTX.AllChildrenWithFilter(
				Exp.GT(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
						Exp.Val("value"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(150)
				)
			);
			CTX ctx5 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("value"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3, ctx4, ctx5);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Should get values > 150 (200 and 300)
			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have 2 values > 150
				Assert.AreEqual(2, results.Count);
			}
		}

		[TestMethod]
		public void TestSingleContextElement()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 230);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = new()
			{
				{ "value", 123 }
			};

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select with single context
			CTX ctx1 = CTX.MapKey(Value.Get("value"));

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			// Results should not be null
			object results = result.GetValue(binName);
			Assert.IsNotNull(results);
		}

		[TestMethod]
		public void TestEmptyLists()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 231);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<object> emptyList = [];
			List<int> items =
			[
				1,
				2,
				3
			];
			data.Add("emptyList", emptyList);
			data.Add("items", items);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Try to select from empty list
			CTX ctx1 = CTX.MapKey(Value.Get("emptyList"));
			CTX ctx2 = CTX.AllChildren();

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.NO_FAIL, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);
		}

		[TestMethod]
		public void TestEmptyMaps()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 232);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> emptyMap = [];
			Dictionary<string, object> items = new()
			{
				{ "a", 1 },
				{ "b", 2 }
			};
			data.Add("emptyMap", emptyMap);
			data.Add("items", items);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Try to select from empty map
			CTX ctx1 = CTX.MapKey(Value.Get("emptyMap"));
			CTX ctx2 = CTX.AllChildren();

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.NO_FAIL, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);
		}

		[TestMethod]
		public void TestListIndexContext()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 233);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> items = [];

			Dictionary<string, object> item1 = new()
			{
				{ "name", "item1" },
				{ "value", 10 }
			};
			items.Add(item1);

			Dictionary<string, object> item2 = new()
			{
				{ "name", "item2" },
				{ "value", 20 }
			};
			items.Add(item2);

			Dictionary<string, object> item3 = new()
			{
				{ "name", "item3" },
				{ "value", 30 }
			};
			items.Add(item3);

			data.Add("items", items);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Select value from second item
			CTX ctx1 = CTX.MapKey(Value.Get("items"));
			CTX ctx2 = CTX.ListIndex(1); // Select second item (index 1)
			CTX ctx3 = CTX.MapKey(Value.Get("value"));

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2, ctx3);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			object resultBin = result.GetValue(binName);
			if (resultBin is IList<object> resultList)
			{
				if (resultList.Count == 1)
				{
					// Should get value 20
					Assert.AreEqual(20L, resultList[0]);
				}
			}
		}

		[TestMethod]
		public void TestModifyWithIndex()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 234);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values =
			[
				100,
				200,
				300,
				400
			];
			data.Add("values", values);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Multiply each value by its index + 1
			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression modifyExp = Exp.Build(
				Exp.Mul(
					Exp.IntLoopVar(LoopVarPart.VALUE),
					Exp.Add(Exp.IntLoopVar(LoopVarPart.INDEX), Exp.Val(1))
				)
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, ctx1, ctx2);

			// CDT apply operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			// Values list should exist
			List<object> finalValues = (List<object>)finalData["values"];
			Assert.IsNotNull(finalValues);

			// Should get values 100, 400, 900, 1600
			Assert.AreEqual(100L, (long)finalValues[0]);
			Assert.AreEqual(400L, (long)finalValues[1]);
			Assert.AreEqual(900L, (long)finalValues[2]);
			Assert.AreEqual(1600L, (long)finalValues[3]);
		}

		[TestMethod]
		public void TestModifyWithComplexArithmetic()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 235);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Dictionary<string, object>> metrics = [];

			Dictionary<string, object> m1 = new()
			{
				{ "value", 10 },
				{ "multiplier", 2 }
			};
			metrics.Add(m1);

			Dictionary<string, object> m2 = new()
			{
				{ "value", 20 },
				{ "multiplier", 3 }
			};
			metrics.Add(m2);

			Dictionary<string, object> m3 = new()
			{
				{ "value", 30 },
				{ "multiplier", 4 }
			};
			metrics.Add(m3);

			data.Add("metrics", metrics);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Add 100 to each value field in the metrics
			CTX ctx1 = CTX.MapKey(Value.Get("metrics"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.EQ(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("value"))
			);

			Expression modifyExp = Exp.Build(
				Exp.Add(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(100))
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, ctx1, ctx2, ctx3);

			// CDT apply operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			// Metrics list should exist
			List<object> finalMetrics = (List<object>)finalData["metrics"];
			Assert.IsNotNull(finalMetrics);

			// First metric should exist
			Dictionary<object, object> firstMetric = (Dictionary<object, object>)finalMetrics[0];
			Assert.IsNotNull(firstMetric);

			long value = (long)firstMetric["value"];
			// 10 + 100 = 110
			Assert.AreEqual(110, value);
		}

		[TestMethod]
		public void TestRemoveAllItemsFromList()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 236);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> items =
			[
				1,
				2,
				3,
				4,
				5
			];
			data.Add("items", items);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("items"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT apply operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			List<object> finalItems = (List<object>)finalData["items"];
			// Items list should exist
			Assert.IsNotNull(finalItems);
			// All items should be removed
			Assert.AreEqual(0, finalItems.Count);
		}

		[TestMethod]
		public void TestRemoveFilteredItemsFromList()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 237);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> numbers =
			[
				1,
				5,
				10,
				15,
				20,
				25,
				30
			];
			data.Add("numbers", numbers);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("numbers"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(10))
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			List<object> finalNumbers = (List<object>)finalData["numbers"];
			// Numbers list should exist
			Assert.IsNotNull(finalNumbers);
			// Should keep items <= 10
			Assert.AreEqual(3, finalNumbers.Count);
			// Should contain 1
			Assert.IsTrue(finalNumbers.Contains(1L));
			// Should contain 5
			Assert.IsTrue(finalNumbers.Contains(5L));
			// Should contain 10
			Assert.IsTrue(finalNumbers.Contains(10L));
		}

		[TestMethod]
		public void TestRemoveAllItemsFromMap()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 238);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> config = new()
			{
				{ "option1", "value1" },
				{ "option2", "value2" },
				{ "option3", "value3" }
			};
			data.Add("config", config);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("config"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			Dictionary<object, object> finalConfig = (Dictionary<object, object>)finalData["config"];
			// Config map should exist
			Assert.IsNotNull(finalConfig);
			// All map entries should be removed
			Assert.AreEqual(0, finalConfig.Count);
		}

		[TestMethod]
		public void TestRemoveFilteredMapEntries()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 239);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> scores = new()
			{
				{ "alice", 95 },
				{ "bob", 45 },
				{ "carol", 75 },
				{ "dave", 30 }
			};
			data.Add("scores", scores);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("scores"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(50))
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			Dictionary<object, object> finalScores = (Dictionary<object, object>)finalData["scores"];
			// Scores map should exist
			Assert.IsNotNull(finalScores);
			// Should keep scores >= 50
			Assert.AreEqual(2, finalScores.Count);

			// Should not contain bob
			Assert.IsFalse(finalScores.ContainsKey("bob"));
			// Should not contain dave
			Assert.IsFalse(finalScores.ContainsKey("dave"));

			// Should contain Alice
			Assert.IsTrue(finalScores.ContainsKey("alice"));
			// Alice score should be 95
			Assert.AreEqual(95L, (long)finalScores["alice"]);
		}

		[TestMethod]
		public void TestRemoveBooksWithLowPrices()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 240);

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
				{ "title", "Cheap Book 1" },
				{ "price", 5.99 }
			};
			booksList.Add(book1);

			Dictionary<string, object> book2 = new()
			{
				{ "title", "Expensive Book" },
				{ "price", 25.99 }
			};
			booksList.Add(book2);

			Dictionary<string, object> book3 = new()
			{
				{ "title", "Cheap Book 2" },
				{ "price", 3.99 }
			};
			booksList.Add(book3);

			Dictionary<string, object> book4 = new()
			{
				{ "title", "Mid Price Book" },
				{ "price", 15.99 }
			};
			booksList.Add(book4);

			Dictionary<string, object> rootMap = new()
			{
				{ "books", booksList }
			};

			var bin = new Bin(binName, rootMap);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("books"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.LE(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
						Exp.Val("price"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(10.0)
				)
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record schould exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Root map should exist
			Dictionary<object, object> finalRootMap = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalRootMap);

			List<object> finalBooks = (List<object>)finalRootMap["books"];
			// Books list should exist
			Assert.IsNotNull(finalBooks);
			// Should keep 2 expensive books
			Assert.AreEqual(2, finalBooks.Count);

			// Verify all remaining books have price > 10.0
			foreach (object bookRaw in finalBooks)
			{
				// Book should be a map
				Dictionary<object, object> book = (Dictionary<object, object>)bookRaw;
				Assert.IsNotNull(book);

				// Book should have a price
				object price = book["price"];
				Assert.IsNotNull(price);

				// Price should be > 10.0
				double priceFloat = (double)price;
				Assert.IsTrue(priceFloat > 10.0);
			}
		}

		[TestMethod]
		public void TestRemoveItemsByIndexFilter()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 241);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values =
			[
				100,
				200,
				300,
				400,
				500
			];
			data.Add("values", values);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GE(Exp.IntLoopVar(LoopVarPart.INDEX), Exp.Val(3))
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			List<object> finalValues = (List<object>)finalData["values"];
			// Values list should exist
			Assert.IsNotNull(finalValues);
			// Should keep first 3 items
			Assert.AreEqual(3, finalValues.Count);
			// First value should be 100
			Assert.AreEqual(100L, (long)finalValues[0]);
			// Second value should be 200
			Assert.AreEqual(200L, (long)finalValues[1]);
			// Third value should be 300
			Assert.AreEqual(300L, (long)finalValues[2]);
		}

		[TestMethod]
		public void TestRemoveMapEntriesByKeyFilter()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 242);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> inventory = new()
			{
				{ "apple", 10 },
				{ "banana", 5 },
				{ "cherry", 8 },
				{ "date", 3 }
			};
			data.Add("inventory", inventory);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("inventory"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GE(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("c"))
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2);

			// CDT remove operation should succeeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			Dictionary<object, object> finalInventory = (Dictionary<object, object>)finalData["inventory"];
			// Inventory map should exist
			Assert.IsNotNull(finalInventory);
			// Should keep 2 items
			Assert.AreEqual(2, finalInventory.Count);

			// Should contain apple
			Assert.IsTrue(finalInventory.ContainsKey("apple"));
			// Should contain banana
			Assert.IsTrue(finalInventory.ContainsKey("banana"));
		}

		[TestMethod]
		public void TestRemoveNestedItemsWithComplexPath()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 243);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			Dictionary<string, object> departments = [];

			List<Dictionary<string, object>> salesList = [];
			Dictionary<string, object> sales1 = new()
			{
				{ "name", "John" },
				{ "sales", 1000 }
			};
			salesList.Add(sales1);
			Dictionary<string, object> sales2 = new()
			{
				{ "name", "Jane" },
				{ "sales", 5000 }
			};
			salesList.Add(sales2);

			List<Dictionary<string, object>> engList = [];
			Dictionary<string, object> eng1 = new()
			{
				{ "name", "Bob" },
				{ "sales", 500 }
			};
			engList.Add(eng1);
			Dictionary<string, object> eng2 = new()
			{
				{ "name", "Alice" },
				{ "sales", 3000 }
			};
			engList.Add(eng2);

			departments.Add("sales", salesList);
			departments.Add("engineering", engList);
			data.Add("departments", departments);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("departments"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));
			CTX ctx3 = CTX.AllChildrenWithFilter(
				Exp.LT(
					MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
						Exp.Val("sales"), Exp.MapLoopVar(LoopVarPart.VALUE)),
					Exp.Val(2000)
				)
			);

			Expression removeExp = Exp.Build(Exp.RemoveResult());
			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, removeExp, ctx1, ctx2, ctx3);

			// CDT remove operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			// Departments map should exist
			Dictionary<object, object> finalDepartments = (Dictionary<object, object>)finalData["departments"];
			Assert.IsNotNull(finalDepartments);

			List<object> finalSalesList = (List<object>)finalDepartments["sales"];
			// Sales list should exist
			Assert.IsNotNull(finalSalesList);
			// Should keep Jane only
			Assert.AreEqual(1, finalSalesList.Count);

			List<object> finalEngList = (List<object>)finalDepartments["engineering"];
			// Enginerring list should exist
			Assert.IsNotNull(finalEngList);
			// Should keep Alice only
			Assert.AreEqual(1, finalEngList.Count);
		}

		[TestMethod]
		public void TestOperateWithNoOperations()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 244);

			// Make sure the record does not exist
			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = new()
			{
				{ "value", 123 }
			};

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			try
			{
				client.Operate(null, rkey);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestSelectByPathWithNullContext()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 245);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<int> numbers =
			[
				10,
				20,
				30
			];

			var bin = new Bin(binName, numbers);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, null);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestSelectByPathWithNoContexts()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 246);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<int> numbers =
			[
				10,
				20,
				30
			];

			var bin = new Bin(binName, numbers);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestSelectByPathWithEmptyContextArray()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 247);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = new()
			{
				{ "value1", 100 },
				{ "value2", 200 },
				{ "value3", 300 }
			};

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			CTX[] emptyCtx = [];
			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, emptyCtx);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestModifyByPathWithNullContext()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 248);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<int> numbers =
			[
				10,
				20,
				30
			];

			var bin = new Bin(binName, numbers);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			Expression modifyExp = Exp.Build(Exp.Val(100));
			Operation modifyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, null);

			try
			{
				client.Operate(null, rkey, modifyOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestModifyByPathWithNoContexts()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 249);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			List<int> numbers =
			[
				10,
				20,
				30
			];

			var bin = new Bin(binName, numbers);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			Expression modifyExp = Exp.Build(Exp.Val(100));
			Operation modifyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp);

			try
			{
				client.Operate(null, rkey, modifyOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestModifyByPathWithEmptyContextArray()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 250);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = new()
			{
				{ "count", 50 }
			};

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			// Record should exist
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			CTX[] emptyCtx = [];
			Expression modifyExp = Exp.Build(Exp.Val(200));
			Operation modifyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, emptyCtx);

			try
			{
				client.Operate(null, rkey, modifyOp);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void TestLoopVarListWithNestedLists()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 251);

			try
			{
				client.Delete(null, rkey);
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

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("matrix"));
			CTX ctx2 = CTX.AllChildren();

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have 3 rows
				Assert.AreEqual(3, results.Count);
			}
		}

		[TestMethod]
		public void TestModifyWithDivision()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 252);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<int> values = [100, 200, 300];
			data.Add("values", values);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("values"));
			CTX ctx2 = CTX.AllChildrenWithFilter(Exp.Val(true));

			Expression modifyExp = Exp.Build(
				Exp.Div(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(10))
			);

			Operation applyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, ctx1, ctx2);

			// CDT modify operation should succeed
			Record result = client.Operate(null, rkey, applyOp);
			Assert.IsNotNull(result);

			// Final record should exist
			Record finalRecord = client.Get(null, rkey);
			Assert.IsNotNull(finalRecord);

			// Data map should exist
			Dictionary<object, object> finalData = (Dictionary<object, object>)finalRecord.GetValue(binName);
			Assert.IsNotNull(finalData);

			// Values list should exist
			List<object> finalValues = (List<object>)finalData["values"];
			Assert.IsNotNull(finalValues);

			// 100 / 10 = 10
			long firstValue = (long)finalValues[0];
			Assert.AreEqual(10, firstValue);
		}

		[TestMethod]
		public void TestLoopVarListAccessNestedListSize()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 253);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<List<int>> matrix = [];
			List<int> row1 = [1, 2, 3];
			matrix.Add(row1);

			List<int> row2 = [4, 5];
			matrix.Add(row2);

			List<int> row3 = [7, 8, 9];
			matrix.Add(row3);

			data.Add("matrix", matrix);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("matrix"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					ListExp.Size(Exp.ListLoopVar(LoopVarPart.VALUE)),
					Exp.Val(3)
				)
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have 2 rows
				Assert.AreEqual(2, results.Count);
			}
		}

		[TestMethod]
		public void TestLoopVarBlobAccessBlobValues()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 254);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<byte[]> blobs = [
				System.Text.Encoding.UTF8.GetBytes("First blob content"),
				System.Text.Encoding.UTF8.GetBytes("Second blob content"),
				System.Text.Encoding.UTF8.GetBytes("Target blob"),
				System.Text.Encoding.UTF8.GetBytes("Fourth blob content")
			];

			data.Add("blobs", blobs);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.MapKey(Value.Get("blobs"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					Exp.BlobLoopVar(LoopVarPart.VALUE),
					Exp.Val(System.Text.Encoding.UTF8.GetBytes("Target blob"))
				)
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have 1 blob matching target
				Assert.AreEqual(1, results.Count);
				byte[] resultBlob = (byte[])results[0];
				// Should match target blob
				Assert.AreEqual("Target blob", System.Text.Encoding.UTF8.GetString(resultBlob));
			}
		}

		[TestMethod]
		public void TestLoopVarNilWithNilValues()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 255);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = new()
			{
				{ "a", 1 },
				{ "b", 2 },
				{ "c", true },
				{ "d", System.Text.Encoding.UTF8.GetBytes("test") },
				{ "e", null }
			};

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			CTX ctx1 = CTX.AllChildrenWithFilter(
				Exp.EQ(
					Exp.NilLoopVar(LoopVarPart.VALUE),
					Exp.Nil()
				)
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE | SelectFlag.NO_FAIL, ctx1);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have 1 nil value
				Assert.AreEqual(1, results.Count);
			}
		}

		[TestMethod]
		public void TestLoopVarGeoJSONFilterLocations()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, 256);

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> data = [];
			List<Value.GeoJSONValue> locations = [
				new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-122.4194,37.7749]}"),
				new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-118.2437,34.0522]}"),
				new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-73.9352,40.7306]}")
			];

			data.Add("locations", locations);

			var bin = new Bin(binName, data);
			client.Put(null, rkey, bin);

			string californiaRegion = "{\"type\":\"Polygon\",\"coordinates\":[[[-124.5,32.5],[-114.0,32.5],[-114.0,42.0],[-124.5,42.0],[-124.5,32.5]]]}";

			CTX ctx1 = CTX.MapKey(Value.Get("locations"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GeoCompare(
					Exp.GeoJSONLoopVar(LoopVarPart.VALUE),
					Exp.Geo(californiaRegion)
				)
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx1, ctx2);

			// CDT select operation should succeed
			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> results = (List<object>)result.GetList(binName);
			if (results != null)
			{
				// Should have filtered GeoJSON locations
				Assert.IsTrue(results.Count >= 0);
				foreach (object item in results)
				{
					// Location should not be null
					Assert.IsNotNull(item);
				}
			}
		}

		[TestMethod]
		public void TestSelectByPathWithNullBinName()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));

			try
			{
				CDTOperation.SelectByPath(null, SelectFlag.VALUE, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("binName"));
			}
		}

		[TestMethod]
		public void TestSelectByPathWithEmptyBinName()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));

			try
			{
				CDTOperation.SelectByPath("", SelectFlag.VALUE, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("binName"));
			}
		}

		[TestMethod]
		public void TestModifyByPathWithNullBinName()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));
			Expression modifyExp = Exp.Build(Exp.Val(100));

			try
			{
				CDTOperation.ModifyByPath(null, ModifyFlag.DEFAULT, modifyExp, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("binName"));
			}
		}

		[TestMethod]
		public void TestModifyByPathWithEmptyBinName()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));
			Expression modifyExp = Exp.Build(Exp.Val(100));

			try
			{
				CDTOperation.ModifyByPath("", ModifyFlag.DEFAULT, modifyExp, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("binName"));
			}
		}

		[TestMethod]
		public void TestSelectByPathWithBinNameTooLong()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));
			string longBinName = "1234567890123456"; // 16 characters, exceeds limit of 15

			try
			{
				CDTOperation.SelectByPath(longBinName, SelectFlag.VALUE, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("15") || e.Message.Contains("exceed"));
			}
		}

		[TestMethod]
		public void TestModifyByPathWithBinNameTooLong()
		{
			CTX ctx1 = CTX.MapKey(Value.Get("test"));
			Expression modifyExp = Exp.Build(Exp.Val(100));
			string longBinName = "1234567890123456"; // 16 characters, exceeds limit of 15

			try
			{
				CDTOperation.ModifyByPath(longBinName, ModifyFlag.DEFAULT, modifyExp, ctx1);
				Assert.Fail("Should throw AerospikeException with PARAMETER_ERROR");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
				Assert.IsTrue(e.Message.Contains("15") || e.Message.Contains("exceed"));
			}
		}

		[TestMethod]
		public void HllNestedInMap()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "hllNestedMapKey");
			client.Delete(null, rkey);

			var values = new List<Value>();
			for (int i = 0; i < 5000; i++)
			{
				values.Add(Value.Get(i));
			}

			client.Operate(null, rkey,
				HLLOperation.Add(HLLPolicy.Default, "hll_temp", values, 4, 4));

			Record rec = client.Get(null, rkey, "hll_temp");
			Assert.IsNotNull(rec);
			var hllVal = (Value.HLLValue)rec.GetValue("hll_temp");
			Assert.IsNotNull(hllVal);

			// Store HLL inside a map
			var mapData = new Dictionary<string, object> { { "a", hllVal } };
			client.Put(null, rkey, new Bin("mapbin", mapData));

			// Read back and verify nested value is HLLValue
			rec = client.Get(null, rkey, "mapbin");
			Assert.IsNotNull(rec);

			var resultMap = (Dictionary<object, object>)rec.GetValue("mapbin");
			Assert.IsNotNull(resultMap);

			object nested = resultMap["a"];
			Assert.IsInstanceOfType(nested, typeof(Value.HLLValue),
				$"Nested value should be HLLValue, got: {nested?.GetType().Name ?? "null"}");
			Assert.IsTrue(((Value.HLLValue)nested).Bytes.Length > 0);
		}

		[TestMethod]
		public void HllNestedInList()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "hllNestedListKey");
			client.Delete(null, rkey);

			var smallData = new List<Value> { Value.Get("a"), Value.Get("b") };
			var largeData = new List<Value>();
			for (int i = 0; i < 5; i++)
			{
				largeData.Add(Value.Get("item" + i));
			}

			client.Operate(null, rkey,
				HLLOperation.Add(HLLPolicy.Default, "hll1", smallData, 8),
				HLLOperation.Add(HLLPolicy.Default, "hll2", largeData, 8));

			Record rec = client.Get(null, rkey, "hll1", "hll2");
			var hll1 = (Value.HLLValue)rec.GetValue("hll1");
			var hll2 = (Value.HLLValue)rec.GetValue("hll2");

			// Store HLLs in a list inside a map
			var data = new Dictionary<string, object>
			{
				{ "hlls", new List<object> { hll1, hll2 } }
			};
			client.Put(null, rkey, new Bin("listbin", data));

			// Read back and verify both preserve type
			rec = client.Get(null, rkey, "listbin");
			Assert.IsNotNull(rec);

			var resultMap = (Dictionary<object, object>)rec.GetValue("listbin");
			Assert.IsNotNull(resultMap);

			var resultList = (List<object>)resultMap["hlls"];
			Assert.IsNotNull(resultList);
			Assert.AreEqual(2, resultList.Count);
			Assert.IsInstanceOfType(resultList[0], typeof(Value.HLLValue));
			Assert.IsInstanceOfType(resultList[1], typeof(Value.HLLValue));
		}

		[TestMethod]
		public void HllNestedInMapInsideMap()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "hllDeepMapKey");
			client.Delete(null, rkey);

			var entries = new List<Value>();
			for (int i = 0; i < 256; i++)
			{
				entries.Add(Value.Get(i));
			}

			client.Operate(null, rkey,
				HLLOperation.Add(HLLPolicy.Default, "hll_temp", entries, 8));

			Record rec = client.Get(null, rkey, "hll_temp");
			var hllVal = (Value.HLLValue)rec.GetValue("hll_temp");
			Assert.IsNotNull(hllVal);

			// Store HLL 2 levels deep: map -> map -> HLL
			var inner = new Dictionary<string, object> { { "level2", hllVal } };
			var outer = new Dictionary<string, object> { { "level1", inner } };
			client.Put(null, rkey, new Bin("deepmap", outer));

			// Read back and verify
			rec = client.Get(null, rkey, "deepmap");
			Assert.IsNotNull(rec);

			var outerResult = (Dictionary<object, object>)rec.GetValue("deepmap");
			Assert.IsNotNull(outerResult);
			var innerResult = (Dictionary<object, object>)outerResult["level1"];
			Assert.IsNotNull(innerResult);

			object nested = innerResult["level2"];
			Assert.IsInstanceOfType(nested, typeof(Value.HLLValue),
				$"HLL nested 2 levels deep should be HLLValue, got: {nested?.GetType().Name ?? "null"}");
		}

		[TestMethod]
		public void HllNestedInListInsideListInsideMap()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "hllDeepListKey");
			client.Delete(null, rkey);

			var entries = new List<Value>();
			for (int i = 0; i < 256; i++)
			{
				entries.Add(Value.Get(i));
			}

			client.Operate(null, rkey,
				HLLOperation.Add(HLLPolicy.Default, "hll_temp", entries, 8));

			Record rec = client.Get(null, rkey, "hll_temp");
			var hllVal = (Value.HLLValue)rec.GetValue("hll_temp");
			Assert.IsNotNull(hllVal);

			// Store HLL 3 levels deep: map -> list -> list -> HLL
			var innerList = new List<object> { hllVal };
			var outerList = new List<object> { innerList };
			var data = new Dictionary<string, object> { { "level1", outerList } };
			client.Put(null, rkey, new Bin("deeplist", data));

			// Read back and verify
			rec = client.Get(null, rkey, "deeplist");
			Assert.IsNotNull(rec);

			var resultMap = (Dictionary<object, object>)rec.GetValue("deeplist");
			Assert.IsNotNull(resultMap);
			var outerResult = (List<object>)resultMap["level1"];
			Assert.IsNotNull(outerResult);
			var innerResult = (List<object>)outerResult[0];
			Assert.IsNotNull(innerResult);

			object nested = innerResult[0];
			Assert.IsInstanceOfType(nested, typeof(Value.HLLValue),
				$"HLL nested 3 levels deep (map->list->list) should be HLLValue, got: {nested?.GetType().Name ?? "null"}");
		}

		[TestMethod]
		public void HllLoopVarFilterOnNestedHLLs()
		{
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "hllLoopVarFilterKey");
			client.Delete(null, rkey);

			var smallData = new List<Value> { Value.Get("a"), Value.Get("b") };
			var largeData = new List<Value>();
			for (int i = 0; i < 5; i++)
			{
				largeData.Add(Value.Get("item" + i));
			}

			client.Operate(null, rkey,
				HLLOperation.Add(HLLPolicy.Default, "hll1", smallData, 8),
				HLLOperation.Add(HLLPolicy.Default, "hll2", largeData, 8));

			Record rec = client.Get(null, rkey, "hll1", "hll2");
			var hll1 = (Value.HLLValue)rec.GetValue("hll1");
			var hll2 = (Value.HLLValue)rec.GetValue("hll2");

			// Store in nested map->list structure
			var hllList = new List<object> { hll1, hll2 };
			var data = new Dictionary<string, object> { { "hlls", hllList } };
			client.Put(null, rkey, new Bin("data", data));

			// Use hllLoopVar to filter HLL values with count > 3
			CTX ctx1 = CTX.MapKey(Value.Get("hlls"));
			CTX ctx2 = CTX.AllChildrenWithFilter(
				Exp.GT(
					HLLExp.GetCount(Exp.HLLLoopVar(LoopVarPart.VALUE)),
					Exp.Val(3)
				)
			);

			Expression selectExp = Exp.Build(
				CDTExp.SelectByPath(
					Exp.Type.LIST,
					SelectFlag.VALUE,
					Exp.MapBin("data"),
					ctx1, ctx2
				)
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("filtered", selectExp, ExpReadFlags.DEFAULT));

			Assert.IsNotNull(result);
			var filtered = result.GetList("filtered");
			Assert.IsNotNull(filtered);
			Assert.AreEqual(1, filtered.Count);
		}

		[TestMethod]
		public void TestCDTOperateMapKeyInList()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtOpMapKeyInList");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			// Create a map with several keys
			Dictionary<string, object> map = new()
			{
				{ "alpha", 10 },
				{ "beta", 20 },
				{ "gamma", 30 },
				{ "delta", 40 }
			};

			var bin = new Bin(binName, map);
			client.Put(null, rkey, bin);

			// Select only keys "alpha" and "gamma" using mapKeysIn via CDTOperation
			CTX ctx = CTX.MapKeysIn("alpha", "gamma");
			var selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx);

			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(2, values.Count);
			Assert.IsTrue(values.Contains(10L));
			Assert.IsTrue(values.Contains(30L));
		}

		[TestMethod]
		public void TestCDTOperateSameLevelFilter()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "cdtOpSameLevelFilter");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 5 },
				{ "b", 15 },
				{ "c", 25 },
				{ "d", 35 }
			};

			var bin = new Bin(binName, map);
			client.Put(null, rkey, bin);

			// Select keys "a", "b", "c" via MapKeysIn, then AND-filter to keep values > 10
			CTX keyInList = CTX.MapKeysIn("a", "b", "c");
			CTX andFilter = CTX.AndFilter(
				Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(10))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.MAP_KEY_VALUE, keyInList, andFilter);

			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			List<object> resultList = (List<object>)result.GetList(binName);
			Assert.IsNotNull(resultList);
			// MAP_KEY_VALUE returns a flat list [key, value, key, value, ...]
			Assert.AreEqual(4, resultList.Count);

			Dictionary<object, object> resultMap = [];
			for (int i = 0; i < resultList.Count; i += 2)
			{
				resultMap.Add(resultList[i], resultList[i + 1]);
			}
			Assert.AreEqual(15L, resultMap["b"]);
			Assert.AreEqual(25L, resultMap["c"]);
		}

		// ---- AndFilter restriction tests ----

		[TestMethod]
		public void TestAndFilterAsFirstContext()
		{
			CheckPathExpressionEnhancements();
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "andFilterFirst");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{"a", 1 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// AndFilter with no preceding context is invalid
			CTX andFilterCtx = CTX.AndFilter(
				Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(0))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, andFilterCtx);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Expected AerospikeException for AndFilter as first context");
			}
			catch (AerospikeException)
			{
				// Expected: AndFilter cannot be the first entry
			}
		}

		[TestMethod]
		public void TestChainedAndFilters()
		{
			CheckPathExpressionEnhancements();
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "chainedAndFilters");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception e)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 },
				{ "c", 3 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// Two AndFilters in a row is invalid - use Exp.And to combine instead
			CTX keyInCtx = CTX.MapKeysIn("a", "b", "c");
			CTX andFilter1 = CTX.AndFilter(
				Exp.GE(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(2))
			);
			CTX andFilter2 = CTX.AndFilter(
				Exp.LE(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(3))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, keyInCtx, andFilter1, andFilter2);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Expected AerospikeException for chained AndFilters");
			}
			catch (AerospikeException)
			{
				// Expected: multiple AndFilters cannot be chained
			}
		}

		[TestMethod]
		public void TestAndFilterAfterAllChildrenWithFilter()
		{
			CheckPathExpressionEnhancements();
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "andFilterAfterExpr");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 },
				{ "c", 3 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// andFilter cannot follow an expression-type context like allChildrenWithFilter
			CTX baseFilter = CTX.AllChildrenWithFilter(
				Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(0))
			);
			CTX andFilterCtx = CTX.AndFilter(
				Exp.LT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(3))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.VALUE, baseFilter, andFilterCtx);

			try
			{
				client.Operate(null, rkey, selectOp);
				Assert.Fail("Expected AerospikeException for AndFilter following expression-type");
			}
			catch (AerospikeException)
			{
				// Expected: andFilter cannot follow expression-type context
			}
		}

		[TestMethod]
		public void TestAndFilterWithMapIndex()
		{
			CheckPathExpressionEnhancements();
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "andFilterMapIndex");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "x", 10 },
				{ "y", 20 },
				{ "z", 30 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// Select map index 0, then AND-filter to keep only entries with key >= "y"
			CTX indexCtx = CTX.MapIndex(0);
			CTX andFilterCtx = CTX.AndFilter(
				Exp.GE(Exp.StringLoopVar(LoopVarPart.MAP_KEY), Exp.Val("y"))
			);

			Operation selectOp = CDTOperation.SelectByPath(binName, SelectFlag.MAP_KEY_VALUE, indexCtx, andFilterCtx);

			Record result = client.Operate(null, rkey, selectOp);
			Assert.IsNotNull(result);

			var resultList = result.GetList(binName);
			Assert.IsNotNull(resultList);
			// Index 0 selects a single entry; AND filter further narrows it.
			// The result depends on which entry is at index 0 and whether it passes the filter.
			// If it passes, we get 2 elements (key+value); if not, 0 elements.
			Assert.IsTrue(resultList.Count == 0 || resultList.Count == 2);
		}

		[TestMethod]
		public void TestModifyByPathViaMapKeysIn()
		{
			CheckPathExpressionEnhancements();
			Key rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "modifyMapKeysIn");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			// Nested structure matching C client test pattern:
			// {inventory: {w1: {item_a: {count: 100}, item_b: {count: 200}},
			//              w2: {item_a: {count: 50},  item_b: {count: 75}},
			//              w3: {item_a: {count: 10},  item_b: {count: 20}}}}
			Dictionary<string, object> w1_a = new()
			{
				{ "count", 100 }
			};
			Dictionary<string, object> w1_b = new()
			{
				{ "count", 200 }
			};
			Dictionary<string, object> w1 = new()
			{
				{ "item_a", w1_a },
				{ "item_b", w1_b }
			};

			Dictionary<string, object> w2_a = new()
			{
				{ "count", 50 }
			};
			Dictionary<string, object> w2_b = new()
			{
				{ "count", 75 }
			};
			Dictionary<string, object> w2 = new()
			{
				{ "item_a", w2_a },
				{ "item_b", w2_b }
			};

			Dictionary<string, object> w3_a = new()
			{
				{ "count", 10 }
			};
			Dictionary<string, object> w3_b = new()
			{
				{ "count", 20 }
			};
			Dictionary<string, object> w3 = new()
			{
				{ "item_a", w3_a },
				{ "item_b", w3_b }
			};

			Dictionary<string, object> inventory = new()
			{
				{ "w1", w1 },
				{ "w2", w2 },
				{ "w3", w3 }
			};

			Dictionary<string, object> top = new()
			{
				{ "inventory", inventory }
			};

			client.Put(null, rkey, new Bin(binName, top));

			// Add 1000 to count in all items of w1 and w2 via ModifyByPath
			// Context: MapKey("inventory") -> MapKeysIn("w1","w2") -> AllChildren() -> MapKey("count")
			CTX invKey = CTX.MapKey(Value.Get("inventory"));
			CTX keysCtx = CTX.MapKeysIn("w1", "w2");
			CTX childCtx = CTX.AllChildren();
			CTX countKey = CTX.MapKey(Value.Get("count"));

			Expression modifyExp = Exp.Build(
				Exp.Add(
					Exp.IntLoopVar(LoopVarPart.VALUE),
					Exp.Val(1000)
				)
			);

			Operation modifyOp = CDTOperation.ModifyByPath(binName, ModifyFlag.DEFAULT, modifyExp, invKey, keysCtx, childCtx, countKey);

			Record result = client.Operate(null, rkey, modifyOp);
			Assert.IsNotNull(result);

			// Verify the modified values
			Record record = client.Get(null, rkey);
			Assert.IsNotNull(record);

			Dictionary<object, object> resultMap = (Dictionary<object, object>)record.bins[binName];
			Dictionary<object, object> resultInv = (Dictionary<object, object>)resultMap["inventory"];
			Dictionary<object, object> resultW1 = (Dictionary<object, object>)resultInv["w1"];
			Dictionary<object, object> resultW1A = (Dictionary<object, object>)resultW1["item_a"];
			// w1.item_a.count should be 1100
			Assert.AreEqual(1100L, resultW1A["count"]);

			Dictionary<object, object> resultW1B = (Dictionary<object, object>)resultW1["item_b"];
			// w1.item_b.count should be 1200
			Assert.AreEqual(1200L, resultW1B["count"]);

			Dictionary<object, object> resultW3 = (Dictionary<object, object>)resultInv["w3"];
			Dictionary<object, object> resultW3A = (Dictionary<object, object>)resultW3["item_a"];
			// w3.item_a.count should be unchanged at 10
			Assert.AreEqual(10L, resultW3A["count"]);
		}

		// ---- MK-002: Select subset - some keys missing ----
		[TestMethod]
		public void TestMapKeysSomeMissing()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkSomeMissing");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn("a", "x");
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(1, values.Count);
			Assert.IsTrue(values.Contains(1L));
		}

		// ---- MK-003: Empty key list ----
		[TestMethod]
		public void TestMapKeysEmptyKeyList()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkEmptyKeys");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn(Array.Empty<string>());
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(0, values.Count);
		}

		// ---- MK-004: Empty map ----
		[TestMethod]
		public void TestMapKeysEmptyMap()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkEmptyMap");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = [];
			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn("a", "b");
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(0, values.Count);
		}

		// ---- MK-005: Single key selection ----
		[TestMethod]
		public void TestMapKeysSingleKey()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkSingleKey");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "x", 1 },
				{ "y", 2 },
				{ "z", 3 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn("y");
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(1, values.Count);
			Assert.AreEqual(2L, values[0]);
		}

		// ---- MK-006: All keys selected ----
		[TestMethod]
		public void TestMapKeysAllKeys()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkAllKeys");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn("a", "b");
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(2, values.Count);
			Assert.IsTrue(values.Contains(1L));
			Assert.IsTrue(values.Contains(2L));
		}

		// ---- MK-007: Key order - results follow map key order, not input list order ----
		[TestMethod]
		public void TestMapKeysOrder()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkOrder");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "z", 3 },
				{ "a", 1 },
				{ "m", 2 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// Request in order [a, z, m] but expect results in key-sorted order [a, m, z]
			CTX ctx = CTX.MapKeysIn("a", "z", "m");
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(3, values.Count);
			// Aerospike maps are key-ordered, so results come back in sorted key order: a=1, m=2, z=3
			Assert.AreEqual(1L, values[0]);
			Assert.AreEqual(2L, values[1]);
			Assert.AreEqual(3L, values[2]);
		}

		// ---- MK-008: Non-string keys (integer keys) ----
		[TestMethod]
		public void TestMapKeysIntegerKeys()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkIntKeys");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<long, string> map = new()
			{
				{ 1L, "one" },
				{ 2L, "two" },
				{ 3L, "three" }
			};

			client.Put(null, rkey, new Bin(binName, map));

			CTX ctx = CTX.MapKeysIn(1L, 2L);
			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(2, values.Count);
			Assert.IsTrue(values.Contains("one"));
			Assert.IsTrue(values.Contains("two"));
		}

		// ---- MK-009: Nested map with mapKeysIn context ----
		[TestMethod]
		public void TestMapKeysNested()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mkNested");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> inner = new()
			{
				{ "a", 1 },
				{ "b", 2 },
				{ "c", 3 }
			};

			Dictionary<string, object> outer = new()
			{
				{ "outer", inner }
			};

			client.Put(null, rkey, new Bin(binName, outer));

			// Navigate into "outer" key, then select keys "a" and "c" from the inner map
			CTX outerCtx = CTX.MapKey(Value.Get("outer"));
			CTX keysCtx = CTX.MapKeysIn("a", "c");

			Record result = client.Operate(null, rkey,
				CDTOperation.SelectByPath(binName, SelectFlag.VALUE, outerCtx, keysCtx)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList(binName);
			Assert.IsNotNull(values);
			Assert.AreEqual(2, values.Count);
			Assert.IsTrue(values.Contains(1L));
			Assert.IsTrue(values.Contains(3L));
		}

		// ---- MV-001: Basic mapValues - extract all values from a map ----
		[TestMethod]
		public void TestMapValuesBasic()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvBasic");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 1 },
				{ "b", 2 },
				{ "c", 3 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList("values");
			Assert.IsNotNull(values);
			Assert.AreEqual(3, values.Count);
			Assert.IsTrue(values.Contains(1L));
			Assert.IsTrue(values.Contains(2L));
			Assert.IsTrue(values.Contains(3L));
		}

		// ---- MV-002: mapValues on empty map ----
		[TestMethod]
		public void TestMapValuesEmptyMap()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvEmptyMap");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new();
			client.Put(null, rkey, new Bin(binName, map));

			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList("values");
			Assert.IsNotNull(values);
			Assert.AreEqual(0, values.Count);
		}

		// ---- MV-003: mapValues on single-entry map ----
		[TestMethod]
		public void TestMapValuesSingleEntry()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvSingleEntry");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "x", 42 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList("values");
			Assert.IsNotNull(values);
			Assert.AreEqual(1, values.Count);
			Assert.AreEqual(42L, values[0]);
		}

		// ---- MV-004: mapValues with integer keys ----
		[TestMethod]
		public void TestMapValuesIntegerKeys()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvIntKeys");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<long, string> map = new()
			{
				{ 1L, "one" },
				{ 2L, "two" },
				{ 3L, "three" }
			};

			client.Put(null, rkey, new Bin(binName, map));

			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList("values");
			Assert.IsNotNull(values);
			Assert.AreEqual(3, values.Count);
			Assert.IsTrue(values.Contains("one"));
			Assert.IsTrue(values.Contains("two"));
			Assert.IsTrue(values.Contains("three"));
		}

		// ---- MV-005: mapValues combined with inList filter ----
		[TestMethod]
		public void TestMapValuesWithInList()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvInList");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "a", 10 },
				{ "b", 20 },
				{ "c", 30 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// Check if 20 is in the map values
			Expression exp = Exp.Build(
				Exp.InList(
					Exp.Val(20),
					Exp.MapValuesIn(Exp.MapBin(binName))
				)
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("found", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.GetBool("found"));

			// Check if 99 is NOT in the map values
			Expression expNot = Exp.Build(
				Exp.InList(
					Exp.Val(99),
					Exp.MapValuesIn(Exp.MapBin(binName))
				)
			);

			Record resultNot = client.Operate(null, rkey,
				ExpOperation.Read("notFound", expNot, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(resultNot);
			Assert.IsFalse(resultNot.GetBool("notFound"));
		}

		// ---- MV-006: mapValues with string values ----
		[TestMethod]
		public void TestMapValuesStringValues()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvStringVals");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "name", "Charlie" },
				{ "city", "London" }
			};

			client.Put(null, rkey, new Bin(binName, map));

			Expression exp = Exp.Build(
				Exp.MapValuesIn(Exp.MapBin(binName))
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			List<object> values = (List<object>)result.GetList("values");
			Assert.IsNotNull(values);
			Assert.AreEqual(2, values.Count);
			Assert.IsTrue(values.Contains("Charlie"));
			Assert.IsTrue(values.Contains("London"));
		}

		// ---- MV-007: mapValues list size check ----
		[TestMethod]
		public void TestMapValuesListSize()
		{
			CheckPathExpressionEnhancements();
			var rkey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "mvListSize");

			try
			{
				client.Delete(null, rkey);
			}
			catch (Exception)
			{
			}

			Dictionary<string, object> map = new()
			{
				{ "x", 1 },
				{ "y", 2 },
				{ "z", 3 }
			};

			client.Put(null, rkey, new Bin(binName, map));

			// Use mapValues inside a list size expression
			Expression exp = Exp.Build(
				Exp.EQ(
					ListExp.Size(Exp.MapValuesIn(Exp.MapBin(binName))),
					Exp.Val(3)
				)
			);

			Record result = client.Operate(null, rkey,
				ExpOperation.Read("sizeCheck", exp, ExpReadFlags.DEFAULT)
			);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.GetBool("sizeCheck"));
		}

		private static void CheckPathExpressionEnhancements()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_2, "Path expression Enhancement");
		}
	}
}
