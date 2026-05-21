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
/// Path expression enhancement examples (requires server 8.1.2+).
/// Demonstrates CTX.MapKeysIn, CTX.AndFilter, Exp.InList,
/// Exp.MapKeysIn, and Exp.MapValuesIn.
/// </summary>
public sealed class PathExpressionEnhanced : SyncExample
{
	private const string MapBinName = "mapbin";

	public override void RunExample()
	{
		RequireMinServerVersion(Node.SERVER_VERSION_8_1_2);

		RunMapKeysSelect();
		RunMapKeysInValueMixedSelect();
		RunMapKeysWithAndFilter();
		RunInListExpression();
		RunMapKeysExpression();
		RunMapValuesExpression();
	}

	/// <summary>
	/// Use CTX.MapKeysIn to select a subset of map entries by key list
	/// via CDTOperation.SelectByPath.
	/// </summary>
	private void RunMapKeysSelect()
	{
		Key key = new(ns, set, "pathexp1");

		Dictionary<string, int> map = new()
		{
			["Charlie"] = 55,
			["Jim"] = 98,
			["John"] = 76,
			["Harry"] = 82
		};

		client.Put(writePolicy, key, new Bin(MapBinName, map));

		CTX ctx = CTX.MapKeysIn("Charlie", "John");
		Record record = client.Operate(writePolicy, key,
			CDTOperation.SelectByPath(MapBinName, SelectFlag.VALUE, ctx));

		console.Info($"SelectByPath MapKeysIn [Charlie, John]: {record.GetList(MapBinName)}");
	}

	/// <summary>
	/// Use <see cref="CTX.MapKeysIn(Value[])"> to select map entries when keys use more than one
	/// CDT type (here: string, integer, and blob) in a single path context. Requires server 8.1.2+.
	/// </summary>
	private void RunMapKeysInValueMixedSelect()
	{
		Key key = new(ns, set, "pathexp6");
		string binName = "mapBin";

		byte[] regionKey = "us-east"u8.ToArray();
		Dictionary<Value, Value> map = new Dictionary<Value, Value>
		{
			{ Value.Get("sku"), Value.Get("standard") },
			{ Value.Get(1001L), Value.Get("express") },
			{ Value.Get(regionKey), Value.Get("regional-offer") }
		};

		client.Operate(null, key,
			MapOperation.PutItems(MapPolicy.Default, binName, map));

		console.Info("Mixed-key map stored (string sku, long 1001, blob region key).");

		CTX ctx = CTX.MapKeysIn(Value.Get("sku"), Value.Get(1001L), Value.Get(regionKey));
		Record record = client.Operate(null, key,
			CDTOperation.SelectByPath(binName, SelectFlag.VALUE, ctx));

		console.Info("selectByPath mapKeysIn(Value...) [sku, 1001, region]: " + record.GetList(binName));
	}

	/// <summary>
	/// Use CTX.MapKeysIn combined with CTX.AndFilter to select map entries
	/// by key list and then further filter by value.
	/// </summary>
	private void RunMapKeysWithAndFilter()
	{
		Key key = new(ns, set, "pathexp2");

		Dictionary<string, int> map = new()
		{
			["Charlie"] = 55,
			["Jim"] = 98,
			["John"] = 76,
			["Harry"] = 82
		};

		client.Put(writePolicy, key, new Bin(MapBinName, map));

		CTX keyCtx = CTX.MapKeysIn("Charlie", "Jim", "John");
		CTX filter = CTX.AndFilter(Exp.GT(Exp.IntLoopVar(LoopVarPart.VALUE), Exp.Val(70)));

		Record record = client.Operate(writePolicy, key,
			CDTOperation.SelectByPath(MapBinName, SelectFlag.MAP_KEY_VALUE, keyCtx, filter));

		console.Info($"SelectByPath MapKeysIn [Charlie, Jim, John] AND value > 70: {record.GetValue(MapBinName)}");
	}

	/// <summary>
	/// Use Exp.InList to check if a bin value is contained in a list.
	/// </summary>
	private void RunInListExpression()
	{
		Key key = new(ns, set, "pathexp3");

		client.Put(writePolicy, key, new Bin("color", "blue"), new Bin("size", 10));

		Expression includesBlue = Exp.Build(
			Exp.InList(
				Exp.StringBin("color"),
				Exp.Val(new List<string> { "red", "blue", "green" })));

		Record record = client.Operate(null, key, ExpOperation.Read("inList", includesBlue, ExpReadFlags.DEFAULT));
		console.Info($"inList [red, blue, green] contains 'blue': {record.GetBool("inList")}");

		Expression excludesBlue = Exp.Build(
			Exp.InList(
				Exp.StringBin("color"),
				Exp.Val(new List<string> { "red", "yellow", "green" })));

		Record recordNot = client.Operate(null, key, ExpOperation.Read("notInList", excludesBlue, ExpReadFlags.DEFAULT));
		console.Info($"inList [red, yellow, green] contains 'blue': {recordNot.GetBool("notInList")}");
	}

	/// <summary>
	/// Use Exp.MapKeysIn to extract all keys from a map bin.
	/// </summary>
	private void RunMapKeysExpression()
	{
		Key key = new(ns, set, "pathexp4");

		Dictionary<string, int> map = new()
		{
			["Charlie"] = 55,
			["Jim"] = 98,
			["John"] = 76
		};
		client.Put(writePolicy, key, new Bin(MapBinName, map));

		Expression exp = Exp.Build(Exp.MapKeysIn(Exp.MapBin(MapBinName)));

		Record record = client.Operate(null, key, ExpOperation.Read("keys", exp, ExpReadFlags.DEFAULT));

		List<object> keys = (List<object>)record.GetList("keys");
		console.Info($"Exp.MapKeysIn: {Util.ListToString(keys)}");
	}

	/// <summary>
	/// Use Exp.MapValuesIn to extract all values from a map bin.
	/// </summary>
	private void RunMapValuesExpression()
	{
		Key key = new(ns, set, "pathexp5");

		Dictionary<string, int> map = new()
		{
			["Charlie"] = 55,
			["Jim"] = 98,
			["John"] = 76
		};

		client.Put(writePolicy, key, new Bin(MapBinName, map));

		Expression exp = Exp.Build(Exp.MapValuesIn(Exp.MapBin(MapBinName)));

		Record record = client.Operate(null, key, ExpOperation.Read("values", exp, ExpReadFlags.DEFAULT));

		List<object> values = (List<object>)record.GetList("values");
		console.Info($"Exp.MapValuesIn: {Util.ListToString(values)}");
	}
}
