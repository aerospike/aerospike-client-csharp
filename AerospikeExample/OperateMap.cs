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

public class OperateMap(Console console) : SyncExample(console)
{

	/// <summary>
	/// Perform operations on a list bin.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RunSimpleExample(client, args);
		RunScoreExample(client, args);
		RunListRangeExample(client, args);
		RunNestedExample(client, args);
		RunNestedMapCreateExample(client, args);
		RunNestedListCreateExample(client, args);

		string mapBinName = args.GetBinName("mapbin");
		Record mapVerify = client.Get(null, new Key(args.ns, args.set, "mapkey3"));
		if (mapVerify == null)
		{
			throw new Exception("OperateMap verification failed: mapkey3 record not found.");
		}
		if (mapVerify.GetValue(mapBinName) == null)
		{
			throw new Exception("OperateMap verification failed: map bin missing.");
		}
		console.Info("OperateMap verified successfully.");
	}

	private void RunSimpleExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

		IDictionary inputMap = new Dictionary<Value, Value>
		{
			[Value.Get(1)] = Value.Get(55),
			[Value.Get(2)] = Value.Get(33)
		};

		// Write values to empty map.
		var record = client.Operate(args.writePolicy, key, MapOperation.PutItems(MapPolicy.Default, binName, inputMap));

		console.Info("Record: " + record);

		// Pop value from map and also return new size of map.
		record = client.Operate(args.writePolicy, key, MapOperation.RemoveByKey(binName, Value.Get(1), MapReturnType.VALUE), MapOperation.Size(binName));

		console.Info("Record: " + record);

		// There should be one result for each map operation on the same map bin.
		// In this case, there are two map operations (pop and size), so there 
		// should be two results.
		IList results = record.GetList(binName);

		foreach (object value in results)
		{
			console.Info("Received: " + value);
		}
	}

	private void RunScoreExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

		IDictionary inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("Charlie")] = Value.Get(55),
			[Value.Get("Jim")] = Value.Get(98),
			[Value.Get("John")] = Value.Get(76),
			[Value.Get("Harry")] = Value.Get(82)
		};

		// Write values to empty map.
		var record = client.Operate(args.writePolicy, key,
			MapOperation.PutItems(MapPolicy.Default, binName, inputMap)
			);

		console.Info("Record: " + record);

		// Increment some user scores.
		record = client.Operate(args.writePolicy, key,
			MapOperation.Increment(MapPolicy.Default, binName, Value.Get("John"), Value.Get(5)),
			MapOperation.Increment(MapPolicy.Default, binName, Value.Get("Jim"), Value.Get(-4))
			);

		console.Info("Record: " + record);

		// Get top two scores.
		record = client.Operate(args.writePolicy, key,
			MapOperation.GetByRankRange(binName, -2, 2, MapReturnType.KEY_VALUE)
			);

		// There should be one result for each map operation on the same map bin.
		// In this case, there are two map operations (pop and size), so there 
		// should be two results.
		IList results = record.GetList(binName);

		foreach (object value in results)
		{
			console.Info("Received: " + value);
		}
	}

	/// <summary>
	/// Value list range example.
	/// </summary>
	private void RunListRangeExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

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

		// Write values to empty map.
		var record = client.Operate(args.writePolicy, key,
			MapOperation.PutItems(MapPolicy.Default, binName, inputMap)
			);

		console.Info("Record: " + record);

		List<Value> end = [MillisSinceEpoch(new DateTime(2018, 2, 2)), Value.AsNull];

		// Delete values < end.
		record = client.Operate(args.writePolicy, key,
			MapOperation.RemoveByValueRange(binName, null, Value.Get(end), MapReturnType.COUNT)
		);

		console.Info("Record: " + record);
	}

	private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

	private static Value MillisSinceEpoch(DateTime dt)
	{
		return Value.Get((long)(dt.ToUniversalTime() - Epoch).TotalMilliseconds);
	}

	/// <summary>
	/// Operate on a map of maps.
	/// </summary>
	private void RunNestedExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey2");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

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

		// Create maps.
		client.Put(args.writePolicy, key, new Bin(binName, inputMap));

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		var record = client.Operate(args.writePolicy, key,
			MapOperation.Put(MapPolicy.Default, binName, Value.Get("key21"), Value.Get(11), CTX.MapKey(Value.Get("key2"))),
			Operation.Get(binName)
			);

		record = client.Get(args.policy, key);
		console.Info("Record: " + record);
	}

	public void RunNestedMapCreateExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey2");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

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

		// Create maps.
		client.Put(args.writePolicy, key, new Bin(binName, inputMap));

		// Create key ordered map at "key2" only if map does not exist.
		// Set map value to 4 for map key "key21" inside of map key "key2".
		CTX ctx = CTX.MapKey(Value.Get("key2"));
		var record = client.Operate(args.writePolicy, key,
			MapOperation.Create(binName, MapOrder.KEY_VALUE_ORDERED, ctx),
			MapOperation.Put(MapPolicy.Default, binName, Value.Get("b"), Value.Get(4), ctx),
			Operation.Get(binName)
			);

		record = client.Get(args.policy, key);
		console.Info("Record: " + record);
	}

	public void RunNestedListCreateExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "mapkey3");
		string binName = args.GetBinName("mapbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

		IList<Value> l1 = [Value.Get(7), Value.Get(9), Value.Get(5)];

		IDictionary<Value, Value> inputMap = new Dictionary<Value, Value>
		{
			[Value.Get("key1")] = Value.Get(l1)
		};

		// Create maps.
		client.Put(args.writePolicy, key, new Bin(binName, inputMap));

		// Create ordered list at map's "key2" only if list does not exist.
		// Append 2,1 to ordered list.
		CTX ctx = CTX.MapKey(Value.Get("key2"));
		var record = client.Operate(args.writePolicy, key,
			ListOperation.Create(binName, ListOrder.ORDERED, false, ctx),
			ListOperation.Append(binName, Value.Get(2), ctx),
			ListOperation.Append(binName, Value.Get(1), ctx),
			Operation.Get(binName)
			);

		record = client.Get(args.policy, key);
		console.Info("Record: " + record);
	}
}
