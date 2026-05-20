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

namespace Aerospike.Example;

/// <summary>
/// Path expression examples using CDTOperation.SelectByPath and
/// CDTExp.ModifyByPath (requires server 8.1.1+). Demonstrates filtering
/// and modifying deeply nested CDT structures using an e-commerce
/// inventory data model.
/// </summary>
public sealed class PathExpression : SyncExample
{
	private const string InventoryBinName = "inventory";

	public override void RunExample()
	{
		RequireMinServerVersion(Node.SERVER_VERSION_8_1_1);

		RunSelectFeaturedInStock();
		RunSelectByMapKeyRegex();
		RunSelectWithMultipleFilters();
		RunModifyByPath();
		RunSelectWithNoFail();
	}

	/// <summary>
	/// SelectByPath: filter featured products with in-stock variants using
	/// MATCHING_TREE to preserve the document structure.
	/// </summary>
	private void RunSelectFeaturedInStock()
	{
		Key key = new(ns, set, "pathexp_qs");

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
		console.Info($"SelectByPath featured + in-stock: found {products.Count} product(s)");

		foreach (KeyValuePair<object, object> entry in products)
		{
			console.Info($"  Product {entry.Key}");
		}
	}

	/// <summary>
	/// SelectByPath: filter map children by key using a regex on the MAP_KEY
	/// loop variable. Selects only product IDs matching "10000.*".
	/// </summary>
	private void RunSelectByMapKeyRegex()
	{
		Key key = new(ns, set, "pathexp_regex");

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
		console.Info($"SelectByPath regex '10000.*': found {products.Count} product(s)");

		foreach (KeyValuePair<object, object> entry in products)
		{
			console.Info($"  Product {entry.Key}");
		}
	}

	/// <summary>
	/// SelectByPath: combine multiple filters with Exp.And to find variants
	/// that are both in stock (quantity > 0) and cheap (price less than 50).
	/// </summary>
	private void RunSelectWithMultipleFilters()
	{
		Key key = new(ns, set, "pathexp_multi");

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
		console.Info($"SelectByPath cheap + in-stock (price<50, qty>0): found {products.Count} product(s)");

		foreach (KeyValuePair<object, object> entry in products)
		{
			console.Info($"  Product {entry.Key}");
		}
	}

	/// <summary>
	/// ModifyByPath: increment the quantity of in-stock variants on featured
	/// products by 10, server-side, without reading the full record first.
	/// Uses CDTExp.ModifyByPath with MapExp.Put to update the nested map.
	/// </summary>
	private void RunModifyByPath()
	{
		Key key = new(ns, set, "pathexp_modify");

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

		console.Info($"ModifyByPath: product 10000001, variant 2001 quantity after +10: {variant2001["quantity"]} (was 100)");
	}

	/// <summary>
	/// SelectByPath with NO_FAIL: tolerate malformed product structures where
	/// a product's "variants" field is not a map/list as expected.
	/// NO_FAIL skips elements that cause type errors instead of failing the operation.
	/// </summary>
	private void RunSelectWithNoFail()
	{
		Key key = new(ns, set, "pathexp_nofail");

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
		console.Info($"SelectByPath with NO_FAIL (malformed product tolerated): found {products.Count} product(s)");

		foreach (KeyValuePair<object, object> entry in products)
		{
			console.Info($"  Product {entry.Key}");
		}
	}
}
