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
using System.Collections;

namespace Aerospike.Example;

public class ListMap(Console console) : SyncExample(console)
{

	/// <summary>
	/// Write List and Map objects.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		TestListStrings(client, args);
		TestListComplex(client, args);
		TestMapStrings(client, args);
		TestMapComplex(client, args);
		TestListMapCombined(client, args);
	}

	/// <summary>
	/// Write/Read List&lt;string&gt; directly instead of relying on default serializer.
	/// </summary>
	private void TestListStrings(IAerospikeClient client, Arguments args)
	{
		console.Info("Read/Write List<string>");
		var key = new Key(args.ns, args.set, "listkey1");
		client.Delete(args.writePolicy, key);

		List<object> list = ["string1", "string2", "string3"];

		var bin = new Bin(args.GetBinName("listbin1"), list);
		client.Put(args.writePolicy, key, bin);

		var record = client.Get(args.policy, key, bin.name);
		var receivedList = (List<object>)record.GetValue(bin.name);

		ValidateSize(3, receivedList.Count);
		Validate("string1", receivedList[0]);
		Validate("string2", receivedList[1]);
		Validate("string3", receivedList[2]);

		console.Info("Read/Write List<string> successful.");
	}

	/// <summary>
	/// Write/Read List&lt;object&gt; directly instead of relying on default serializer.
	/// </summary>
	private void TestListComplex(IAerospikeClient client, Arguments args)
	{
		console.Info("Read/Write List<object>");
		var key = new Key(args.ns, args.set, "listkey2");
		client.Delete(args.writePolicy, key);

		byte[] blob = [3, 52, 125];
		List<object> list = ["string1", 2, blob];

		var bin = new Bin(args.GetBinName("listbin2"), list);
		client.Put(args.writePolicy, key, bin);

		var record = client.Get(args.policy, key, bin.name);
		var receivedList = (List<object>)record.GetValue(bin.name);

		ValidateSize(3, receivedList.Count);
		Validate("string1", receivedList[0]);
		// Server convert numbers to long, so must expect long.
		Validate(2L, receivedList[1]);
		Validate(blob, (byte[])receivedList[2]);

		console.Info("Read/Write List<object> successful.");
	}

	/// <summary>
	/// Write/Read Dictionary&lt;string, string&gt; directly instead of relying on default serializer.
	/// </summary>
	private void TestMapStrings(IAerospikeClient client, Arguments args)
	{
		console.Info("Read/Write Dictionary<string, string>");
		var key = new Key(args.ns, args.set, "mapkey1");
		client.Delete(args.writePolicy, key);

		var map = new Dictionary<object, object>
		{
			["key1"] = "string1",
			["key2"] = "string2",
			["key3"] = "string3"
		};

		var bin = new Bin(args.GetBinName("mapbin1"), map);
		client.Put(args.writePolicy, key, bin);

		var record = client.Get(args.policy, key, bin.name);
		var receivedMap = (Dictionary<object, object>)record.GetValue(bin.name);

		ValidateSize(3, receivedMap.Count);
		Validate("string1", receivedMap["key1"]);
		Validate("string2", receivedMap["key2"]);
		Validate("string3", receivedMap["key3"]);

		console.Info("Read/Write Dictionary<string, string> successful");
	}

	/// <summary>
	/// Write/Read Dictionary&lt;object, object&gt; directly instead of relying on default serializer.
	/// </summary>
	private void TestMapComplex(IAerospikeClient client, Arguments args)
	{
		console.Info("Read/Write Dictionary<object, object>");
		var key = new Key(args.ns, args.set, "mapkey2");
		client.Delete(args.writePolicy, key);

		byte[] blob = [3, 52, 125];
		List<int> list = [100034, 12384955, 3, 512];

		var map = new Dictionary<object, object>
		{
			["key1"] = "string1",
			["key2"] = 2,
			["key3"] = blob,
			["key4"] = list
		};

		var bin = new Bin(args.GetBinName("mapbin2"), map);
		client.Put(args.writePolicy, key, bin);

		var record = client.Get(args.policy, key, bin.name);
		var receivedMap = (Dictionary<object, object>)record.GetValue(bin.name);

		ValidateSize(4, receivedMap.Count);
		Validate("string1", receivedMap["key1"]);
		// Server convert numbers to long, so must expect long.
		Validate(2L, receivedMap["key2"]);
		Validate(blob, (byte[])receivedMap["key3"]);

		var receivedInner = (IList)receivedMap["key4"];
		ValidateSize(4, receivedInner.Count);
		Validate(100034L, receivedInner[0]);
		Validate(12384955L, receivedInner[1]);
		Validate(3L, receivedInner[2]);
		Validate(512L, receivedInner[3]);

		console.Info("Read/Write Dictionary<object, object> successful");
	}

	/// <summary>
	/// Write/Read List/Dictionary combination directly instead of relying on default serializer.
	/// </summary>
	private void TestListMapCombined(IAerospikeClient client, Arguments args)
	{
		console.Info("Read/Write List/Dictionary");
		var key = new Key(args.ns, args.set, "listmapkey");
		client.Delete(args.writePolicy, key);

		byte[] blob = [3, 52, 125];
		List<object> inner = ["string2", 5];

		var innerMap = new Dictionary<object, object>
		{
			["a"] = 1,
			[2] = "b",
			[3] = blob,
			["list"] = inner
		};

		List<object> list = ["string1", 8, inner, innerMap];

		var bin = new Bin(args.GetBinName("listmapbin"), list);
		client.Put(args.writePolicy, key, bin);

		var record = client.Get(args.policy, key, bin.name);
		var received = (List<object>)record.GetValue(bin.name);

		ValidateSize(4, received.Count);
		Validate("string1", received[0]);
		// Server convert numbers to long, so must expect long.
		Validate(8L, received[1]);

		var receivedInner = (List<object>)received[2];
		ValidateSize(2, receivedInner.Count);
		Validate("string2", receivedInner[0]);
		Validate(5L, receivedInner[1]);

		var receivedMap = (Dictionary<object, object>)received[3];
		ValidateSize(4, receivedMap.Count);
		Validate(1L, receivedMap["a"]);
		Validate("b", receivedMap[2L]);
		Validate(blob, (byte[])receivedMap[3L]);

		var receivedInner2 = (List<object>)receivedMap["list"];
		ValidateSize(2, receivedInner2.Count);
		Validate("string2", receivedInner2[0]);
		Validate(5L, receivedInner2[1]);

		console.Info("Read/Write List/Dictionary successful");
	}

	private static void ValidateSize(int expected, int received)
	{
		if (received != expected)
		{
			throw new Exception($"Size mismatch: expected={expected} received={received}");
		}
	}

	private static void Validate(object expected, object received)
	{
		if (!received.Equals(expected))
		{
			throw new Exception($"Mismatch: expected={expected} received={received}");
		}
	}

	private static void Validate(byte[] expected, byte[] received)
	{
		string expectedString = Util.BytesToString(expected);
		string receivedString = Util.BytesToString(received);

		if (!expectedString.Equals(receivedString))
		{
			throw new Exception($"Mismatch: expected={expectedString} received={receivedString}");
		}
	}
}
