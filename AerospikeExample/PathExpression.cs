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
using System.Collections.Generic;

namespace Aerospike.Example;

/// <summary>
/// Path expression examples using CDTOperation.SelectByPath and
/// CDTExp.ModifyByPath (requires server 8.1.1+).
/// Demonstrates filtering and modifying deeply nested CDT structures
/// using an e-commerce inventory data model.
/// </summary>
internal class PathExpression(Console console) : SyncExample(console)
{
	private const string InventoryBinName = "inventory";

	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RequireMinServerVersion(args, Node.SERVER_VERSION_8_1_1);

		RunSelectFeaturedInStock(client, args);
		RunSelectByMapKeyRegex(client, args);
		RunSelectWithMultipleFilters(client, args);
		RunModifyByPath(client, args);
		RunSelectWithNoFail(client, args);

		Key catalogKey = new Key(args.ns, args.set, "pathexp_modify");
		Record catalogRec = client.Get(null, catalogKey);
		if (catalogRec == null)
		{
			throw new Exception("PathExpression verification failed: pathexp_modify record not found.");
		}
		var root = (Dictionary<object, object>)catalogRec.GetMap(InventoryBinName);
		if (root == null || !root.TryGetValue("inventory", out object invRoot) || invRoot is not Dictionary<object, object> products)
		{
			throw new Exception("PathExpression verification failed: inventory bin missing expected structure.");
		}
		if (!products.ContainsKey("10000001"))
		{
			throw new Exception("PathExpression verification failed: expected catalog product 10000001 missing.");
		}
		console.Info("PathExpression verified successfully.");
	}

	/// <summary>
	/// SelectByPath: filter featured products with in-stock variants using
	/// MATCHING_TREE to preserve the document structure.
	/// </summary>
	private void RunSelectFeaturedInStock(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "pathexp_qs");
		SetupInventorySample(client, key, extraProduct: false);

		Exp filterOnFeatured = Exp.EQ(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.BOOL,
				Exp.Val("featured"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(true)
		);

		Exp filterOnVariantInventory = Exp.GT(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.INT,
				Exp.Val("quantity"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(0)
		);

		Record record = client.Operate(null, key,
			CDTOperation.SelectByPath(InventoryBinName, SelectFlag.MATCHING_TREE,
				CTX.AllChildren(),
				CTX.AllChildrenWithFilter(filterOnFeatured),
				CTX.MapKey(Value.Get("variants")),
				CTX.AllChildrenWithFilter(filterOnVariantInventory)
			)
		);

		Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(InventoryBinName);
		Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
		console.Info("SelectByPath featured + in-stock: found {0} product(s)", products.Count);

		foreach (var entry in products)
		{
			console.Info("  Product {0}", entry.Key);
		}
	}

	/// <summary>
	/// SelectByPath: filter map children by key using a regex on the MAP_KEY
	/// loop variable. Selects only product IDs matching "10000.*".
	/// </summary>
	private void RunSelectByMapKeyRegex(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "pathexp_regex");
		SetupInventorySample(client, key, extraProduct: false);

		Exp filterOnKey = Exp.RegexCompare(
			"10000.*", RegexFlag.NONE, Exp.StringLoopVar(LoopVarPart.MAP_KEY)
		);

		Record record = client.Operate(null, key,
			CDTOperation.SelectByPath(InventoryBinName, SelectFlag.MATCHING_TREE,
				CTX.AllChildren(),
				CTX.AllChildrenWithFilter(filterOnKey)
			)
		);

		Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(InventoryBinName);
		Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
		console.Info("SelectByPath regex '10000.*': found {0} product(s)", products.Count);

		foreach (var entry in products)
		{
			console.Info("  Product {0}", entry.Key);
		}
	}

	/// <summary>
	/// SelectByPath: combine multiple filters with Exp.And to find variants
	/// that are both in stock (quantity > 0) and cheap (price less than 50).
	/// </summary>
	private void RunSelectWithMultipleFilters(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "pathexp_multi");
		SetupInventorySample(client, key, extraProduct: false);

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

		Record record = client.Operate(null, key,
			CDTOperation.SelectByPath(InventoryBinName, SelectFlag.MATCHING_TREE,
				CTX.AllChildren(),
				CTX.AllChildren(),
				CTX.MapKey(Value.Get("variants")),
				CTX.AllChildrenWithFilter(filterOnCheapInStock)
			)
		);

		Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(InventoryBinName);
		Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
		console.Info("SelectByPath cheap + in-stock (price<50, qty>0): found {0} product(s)", products.Count);

		foreach (var entry in products)
		{
			console.Info("  Product {0}", entry.Key);
		}
	}

	/// <summary>
	/// ModifyByPath: increment the quantity of in-stock variants on featured
	/// products by 10, server-side, without reading the full record first.
	/// Uses CDTExp.ModifyByPath with MapExp.Put to update the nested map.
	/// </summary>
	private void RunModifyByPath(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "pathexp_modify");
		SetupInventorySample(client, key, extraProduct: false);

		Exp incrementExp = MapExp.Put(
			MapPolicy.Default,
			Exp.Val("quantity"),
			Exp.Add(
				MapExp.GetByKey(MapReturnType.VALUE, Exp.Type.INT,
					Exp.Val("quantity"),
					Exp.MapLoopVar(LoopVarPart.VALUE)),
				Exp.Val(10)
			),
			Exp.MapLoopVar(LoopVarPart.VALUE)
		);

		Exp filterOnFeatured = Exp.EQ(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.BOOL,
				Exp.Val("featured"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(true)
		);

		Exp filterOnVariantInventory = Exp.GT(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.INT,
				Exp.Val("quantity"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(0)
		);

		Expression modifyExpression = Exp.Build(
			CDTExp.ModifyByPath(
				Exp.Type.MAP,
				ModifyFlag.DEFAULT,
				incrementExp,
				Exp.MapBin(InventoryBinName),
				CTX.AllChildren(),
				CTX.AllChildrenWithFilter(filterOnFeatured),
				CTX.MapKey(Value.Get("variants")),
				CTX.AllChildrenWithFilter(filterOnVariantInventory)
			)
		);

		string updatedBin = "upd_inventory";
		client.Operate(null, key,
			ExpOperation.Write(updatedBin, modifyExpression, ExpWriteFlags.DEFAULT));

		Record updatedRecord = client.Get(null, key);
		Dictionary<object, object> resultMap = (Dictionary<object, object>)updatedRecord.GetMap(updatedBin);
		Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];

		Dictionary<object, object> product1 = (Dictionary<object, object>)products["10000001"];
		Dictionary<object, object> variants = (Dictionary<object, object>)product1["variants"];
		Dictionary<object, object> variant2001 = (Dictionary<object, object>)variants["2001"];

		console.Info("ModifyByPath: product 10000001, variant 2001 quantity after +10: {0} (was 100)",
			variant2001["quantity"]);
	}

	/// <summary>
	/// SelectByPath with NO_FAIL: tolerate malformed product structures where
	/// a product's "variants" field is not a map/list as expected.
	/// NO_FAIL skips elements that cause type errors instead of failing the operation.
	/// </summary>
	private void RunSelectWithNoFail(IAerospikeClient client, Arguments args)
	{
		Key key = new Key(args.ns, args.set, "pathexp_nofail");
		SetupInventorySample(client, key, extraProduct: true);

		Exp filterOnFeatured = Exp.EQ(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.BOOL,
				Exp.Val("featured"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(true)
		);

		Exp filterOnVariantInventory = Exp.GT(
			MapExp.GetByKey(
				MapReturnType.VALUE, Exp.Type.INT,
				Exp.Val("quantity"),
				Exp.MapLoopVar(LoopVarPart.VALUE)
			),
			Exp.Val(0)
		);

		Record record = client.Operate(null, key,
			CDTOperation.SelectByPath(InventoryBinName, SelectFlag.MATCHING_TREE | SelectFlag.NO_FAIL,
				CTX.AllChildren(),
				CTX.AllChildrenWithFilter(filterOnFeatured),
				CTX.MapKey(Value.Get("variants")),
				CTX.AllChildrenWithFilter(filterOnVariantInventory)
			)
		);

		Dictionary<object, object> resultMap = (Dictionary<object, object>)record.GetMap(InventoryBinName);
		Dictionary<object, object> products = (Dictionary<object, object>)resultMap["inventory"];
		console.Info("SelectByPath with NO_FAIL (malformed product tolerated): found {0} product(s)", products.Count);

		foreach (var entry in products)
		{
			console.Info("  Product {0}", entry.Key);
		}
	}

	/// <summary>
	/// Set up the inventory sample data structure used by the path expression
	/// examples that match the aerospike-websites documentation tutorials.
	/// </summary>
	private static void SetupInventorySample(IAerospikeClient client, Key key, bool extraProduct)
	{
		client.Delete(null, key);

		Dictionary<string, object> inventory = new();

		// Product 10000001: Classic T-Shirt (featured)
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

		// Product 10000002: Casual Polo Shirt (not featured)
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

		// Product 50000006: Laptop Pro 14 (featured, out of stock)
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

		// Product 50000009: Smart TV (featured, list-based variants)
		Dictionary<string, object> product4 = new()
		{
			{ "category", "electronics" },
			{ "featured", true },
			{ "name", "Smart TV" },
			{ "description", "Ultra HD smart television with built-in streaming apps." }
		};
		List<Dictionary<string, object>> product4Variants = new()
		{
			new() { { "sku", 3007 }, { "spec", "1080p" }, { "price", 199 }, { "quantity", 60 } },
			new() { { "sku", 3008 }, { "spec", "4K" }, { "price", 399 }, { "quantity", 30 } }
		};
		product4.Add("variants", product4Variants);
		inventory.Add("50000009", product4);

		if (extraProduct)
		{
			// Product 10000003: Hooded Sweatshirt (featured, malformed variants for NO_FAIL demo)
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

		Dictionary<string, object> data = new()
		{
			{ "inventory", inventory }
		};

		client.Put(null, key, new Bin(InventoryBinName, data));
	}
}
