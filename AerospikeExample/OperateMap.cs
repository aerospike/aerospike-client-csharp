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

public sealed class OperateMap : SyncExample
{
	private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

	/// <summary>
	/// Perform operations on map bins.
	/// </summary>
	public override void RunExample()
	{
		RunSimpleExample();
		RunScoreExample();
		RunListRangeExample();
		RunNestedExample();
		RunNestedMapCreateExample();
		RunNestedListCreateExample();
	}

	private void RunSimpleExample()
	{
		Key key = new(ns, set, "mapkey_score");
		string binName = "mapbin";

		IDictionary inputMap = new Dictionary<Value, Value>
		{
			[Value.Get(1)] = Value.Get(55),
			[Value.Get(2)] = Value.Get(33)
		};

		Record record = client.Operate(writePolicy, key,
			MapOperation.PutItems(MapPolicy.Default, binName, inputMap));
		console.Info($"Record: {record}");

		// Remove a key, returning its value, and report the new map size.
		record = client.Operate(writePolicy, key,
			MapOperation.RemoveByKey(binName, Value.Get(1), MapReturnType.VALUE),
			MapOperation.Size(binName));
		console.Info($"Record: {record}");

		IList results = record.GetList(binName);

		foreach (object value in results)
		{
			console.Info($"Received: {value}");
		}
	}

	private void RunScoreExample()
	{
		Key key = new(ns, set, "mapkey_range");
		string binName = "mapbin";

		IDictionary inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("Charlie")] = Value.Get(55),
			[Value.Get("Jim")] = Value.Get(98),
			[Value.Get("John")] = Value.Get(76),
			[Value.Get("Harry")] = Value.Get(82)
		};

		Record record = client.Operate(writePolicy, key,
			MapOperation.PutItems(MapPolicy.Default, binName, inputMap));
		console.Info($"Record: {record}");

		// Increment two user scores.
		record = client.Operate(writePolicy, key,
			MapOperation.Increment(MapPolicy.Default, binName, Value.Get("John"), Value.Get(5)),
			MapOperation.Increment(MapPolicy.Default, binName, Value.Get("Jim"), Value.Get(-4)));
		console.Info($"Record: {record}");

		// Read the top two scores.
		record = client.Operate(writePolicy, key,
			MapOperation.GetByRankRange(binName, -2, 2, MapReturnType.KEY_VALUE));

		IList results = record.GetList(binName);

		foreach (object value in results)
		{
			console.Info($"Received: {value}");
		}
	}

	/// <summary>
	/// Use a list value range to delete all entries with values below a threshold.
	/// </summary>
	private void RunListRangeExample()
	{
		Key key = new(ns, set, "mapkey");
		string binName = "mapbin";

		List<Value> l1 = [MillisSinceEpoch(new DateTime(2018, 1, 1)), Value.Get(1)];
		List<Value> l2 = [MillisSinceEpoch(new DateTime(2018, 1, 2)), Value.Get(2)];
		List<Value> l3 = [MillisSinceEpoch(new DateTime(2018, 2, 1)), Value.Get(3)];
		List<Value> l4 = [MillisSinceEpoch(new DateTime(2018, 2, 2)), Value.Get(4)];
		List<Value> l5 = [MillisSinceEpoch(new DateTime(2018, 2, 5)), Value.Get(5)];

		IDictionary inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("Charlie")] = Value.Get(l1),
			[Value.Get("Jim")] = Value.Get(l2),
			[Value.Get("John")] = Value.Get(l3),
			[Value.Get("Harry")] = Value.Get(l4),
			[Value.Get("Bill")] = Value.Get(l5)
		};

		Record record = client.Operate(writePolicy, key,
			MapOperation.PutItems(MapPolicy.Default, binName, inputMap));
		console.Info($"Record: {record}");

		List<Value> end = [MillisSinceEpoch(new DateTime(2018, 2, 2)), Value.AsNull];

		record = client.Operate(writePolicy, key,
			MapOperation.RemoveByValueRange(binName, null, Value.Get(end), MapReturnType.COUNT));
		console.Info($"Record: {record}");
	}

	private static Value MillisSinceEpoch(DateTime dt)
		=> Value.Get((long)(dt.ToUniversalTime() - Epoch).TotalMilliseconds);

	/// <summary>
	/// Operate on a map of maps using a context to navigate to the inner map.
	/// </summary>
	private void RunNestedExample()
	{
		Key key = new(ns, set, "mapkey_nested");
		string binName = "mapbin";

		IDictionary<Value, Value> m1 = new Dictionary<Value, Value>
		{
			[Value.Get("key11")] = Value.Get(9),
			[Value.Get("key12")] = Value.Get(4)
		};

		IDictionary<Value, Value> m2 = new Dictionary<Value, Value>
		{
			[Value.Get("key21")] = Value.Get(3),
			[Value.Get("key22")] = Value.Get(5)
		};

		IDictionary<Value, Value> inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("key1")] = Value.Get(m1),
			[Value.Get("key2")] = Value.Get(m2)
		};

		client.Put(writePolicy, key, new Bin(binName, inputMap));

		// Update key21 inside the map at key2 and retrieve the full bin.
		client.Operate(writePolicy, key,
			MapOperation.Put(MapPolicy.Default, binName, Value.Get("key21"), Value.Get(11), CTX.MapKey(Value.Get("key2"))),
			Operation.Get(binName));

		Record record = client.Get(policy, key);
		console.Info($"Record: {record}");
	}

	private void RunNestedMapCreateExample()
	{
		Key key = new(ns, set, "mapkey_nested_create");
		string binName = "mapbin";

		IDictionary<Value, Value> m1 = new Dictionary<Value, Value>
		{
			[Value.Get("key21")] = Value.Get(7),
			[Value.Get("key22")] = Value.Get(6)
		};

		IDictionary<Value, Value> m2 = new Dictionary<Value, Value>
		{
			[Value.Get("a")] = Value.Get(3),
			[Value.Get("c")] = Value.Get(5)
		};

		IDictionary<Value, Value> inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("key1")] = Value.Get(m1),
			[Value.Get("key2")] = Value.Get(m2)
		};

		client.Put(writePolicy, key, new Bin(binName, inputMap));

		// Conditionally create a key-ordered map at key2 and add a new entry.
		CTX ctx = CTX.MapKey(Value.Get("key2"));
		client.Operate(writePolicy, key,
			MapOperation.Create(binName, MapOrder.KEY_VALUE_ORDERED, ctx),
			MapOperation.Put(MapPolicy.Default, binName, Value.Get("b"), Value.Get(4), ctx),
			Operation.Get(binName));

		Record record = client.Get(policy, key);
		console.Info($"Record: {record}");
	}

	private void RunNestedListCreateExample()
	{
		Key key = new(ns, set, "mapkey3");
		string binName = "mapbin";

		IList<Value> l1 = [Value.Get(7), Value.Get(9), Value.Get(5)];

		IDictionary<Value, Value> inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("key1")] = Value.Get(l1)
		};

		client.Put(writePolicy, key, new Bin(binName, inputMap));

		// Conditionally create an ordered list at key2 and append two values.
		CTX ctx = CTX.MapKey(Value.Get("key2"));
		client.Operate(writePolicy, key,
			ListOperation.Create(binName, ListOrder.ORDERED, false, ctx),
			ListOperation.Append(binName, Value.Get(2), ctx),
			ListOperation.Append(binName, Value.Get(1), ctx),
			Operation.Get(binName));

		Record record = client.Get(policy, key);
		console.Info($"Record: {record}");
	}
}
