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

public sealed class ListMap : SyncExample
{
	/// <summary>
	/// Write and read List and Map objects in their native CLR form.
	/// </summary>
	public override void RunExample()
	{
		RoundTripListStrings();
		RoundTripListComplex();
		RoundTripMapStrings();
		RoundTripMapComplex();
		RoundTripListMapCombined();
	}

	private void RoundTripListStrings()
	{
		console.Info("Read/Write List<string>");
		Key key = new(ns, set, "listkey1");
		Bin bin = new("listbin1", new List<object> { "string1", "string2", "string3" });

		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key, bin.name);
		console.Info($"Read/Write List<string>: {record?.GetValue(bin.name)}");
	}

	private void RoundTripListComplex()
	{
		console.Info("Read/Write List<object>");
		Key key = new(ns, set, "listkey2");

		byte[] blob = [3, 52, 125];
		Bin bin = new("listbin2", new List<object> { "string1", 2, blob });

		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key, bin.name);
		console.Info($"Read/Write List<object>: {record?.GetValue(bin.name)}");
	}

	private void RoundTripMapStrings()
	{
		console.Info("Read/Write Dictionary<string, string>");
		Key key = new(ns, set, "mapkey1");

		Dictionary<object, object> map = new()
		{
			["key1"] = "string1",
			["key2"] = "string2",
			["key3"] = "string3"
		};
		Bin bin = new("mapbin1", map);
		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key, bin.name);
		console.Info($"Read/Write Dictionary<string, string>: {record?.GetValue(bin.name)}");
	}

	private void RoundTripMapComplex()
	{
		console.Info("Read/Write Dictionary<object, object>");
		Key key = new(ns, set, "mapkey2");

		byte[] blob = [3, 52, 125];
		List<int> list = [100034, 12384955, 3, 512];

		Dictionary<object, object> map = new()
		{
			["key1"] = "string1",
			["key2"] = 2,
			["key3"] = blob,
			["key4"] = list
		};

		Bin bin = new("mapbin2", map);
		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key, bin.name);
		console.Info($"Read/Write Dictionary<object, object>: {record?.GetValue(bin.name)}");
	}

	private void RoundTripListMapCombined()
	{
		console.Info("Read/Write List/Dictionary");
		Key key = new(ns, set, "listmapkey");

		byte[] blob = [3, 52, 125];
		List<object> inner = ["string2", 5];

		Dictionary<object, object> innerMap = new()
		{
			["a"] = 1,
			[2] = "b",
			[3] = blob,
			["list"] = inner
		};

		List<object> list = ["string1", 8, inner, innerMap];

		Bin bin = new("listmapbin", list);
		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key, bin.name);
		console.Info($"Read/Write List/Dictionary: {record?.GetValue(bin.name)}");
	}
}
