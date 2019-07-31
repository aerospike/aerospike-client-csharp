/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class OperateMap : SyncExample
	{
		public OperateMap(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Perform operations on a list bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (! args.hasCDTMap)
			{
				console.Info("CDT map functions are not supported by the connected Aerospike server.");
				return;
			}	
			RunSimpleExample(client, args);
			RunScoreExample(client, args);
			RunListRangeExample(client, args);
			RunNestedExample(client, args);
		}

		private void RunSimpleExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "mapkey");
			string binName = args.GetBinName("mapbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IDictionary inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get(1)] = Value.Get(55);
			inputMap[Value.Get(2)] = Value.Get(33);

			// Write values to empty map.
			Record record = client.Operate(args.writePolicy, key, MapOperation.PutItems(MapPolicy.Default, binName, inputMap));

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

		private void RunScoreExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "mapkey");
			string binName = args.GetBinName("mapbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IDictionary inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(55);
			inputMap[Value.Get("Jim")] = Value.Get(98);
			inputMap[Value.Get("John")] = Value.Get(76);
			inputMap[Value.Get("Harry")] = Value.Get(82);

			// Write values to empty map.
			Record record = client.Operate(args.writePolicy, key,
				MapOperation.PutItems(MapPolicy.Default, binName, inputMap)
				);

			console.Info("Record: " + record);

			// Increment some user scores.
			record = client.Operate(args.writePolicy, key,
				MapOperation.Increment(MapPolicy.Default, binName, Value.Get("John"), Value.Get(5)),
				MapOperation.Decrement(MapPolicy.Default, binName, Value.Get("Jim"), Value.Get(4))
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
		private void RunListRangeExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "mapkey");
			string binName = args.GetBinName("mapbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			List<Value> l1 = new List<Value>();
			l1.Add(MillisSinceEpoch(new DateTime(2018, 1, 1)));
			l1.Add(Value.Get(1));

			List<Value> l2 = new List<Value>();
			l2.Add(MillisSinceEpoch(new DateTime(2018, 1, 2)));
			l2.Add(Value.Get(2));

			List<Value> l3 = new List<Value>();
			l3.Add(MillisSinceEpoch(new DateTime(2018, 2, 1)));
			l3.Add(Value.Get(3));

			List<Value> l4 = new List<Value>();
			l4.Add(MillisSinceEpoch(new DateTime(2018, 2, 2)));
			l4.Add(Value.Get(4));

			List<Value> l5 = new List<Value>();
			l5.Add(MillisSinceEpoch(new DateTime(2018, 2, 5)));
			l5.Add(Value.Get(5));

			IDictionary inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(l1);
			inputMap[Value.Get("Jim")] = Value.Get(l2);
			inputMap[Value.Get("John")] = Value.Get(l3);
			inputMap[Value.Get("Harry")] = Value.Get(l4);
			inputMap[Value.Get("Bill")] = Value.Get(l5);

			// Write values to empty map.
			Record record = client.Operate(args.writePolicy, key,
				MapOperation.PutItems(MapPolicy.Default, binName, inputMap)
				);

			console.Info("Record: " + record);

			List<Value> end = new List<Value>();
			end.Add(MillisSinceEpoch(new DateTime(2018, 2, 2)));
			end.Add(Value.AsNull);

			// Delete values < end.
			record = client.Operate(args.writePolicy, key,
				MapOperation.RemoveByValueRange(binName, null, Value.Get(end), MapReturnType.COUNT)
			);

			console.Info("Record: " + record);
		}

		private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		
		private static Value MillisSinceEpoch(DateTime dt)
		{
			return Value.Get((long)(dt.ToUniversalTime() - Epoch).TotalMilliseconds);
		}

		/// <summary>
		/// Operate on a map of maps.
		/// </summary>
		private void RunNestedExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "mapkey2");
			string binName = args.GetBinName("mapbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IDictionary<Value, Value> m1 = new Dictionary<Value, Value>();
			m1[Value.Get("key11")] = Value.Get(9);
			m1[Value.Get("key12")] = Value.Get(4);

			IDictionary<Value, Value> m2 = new Dictionary<Value, Value>();
			m2[Value.Get("key21")] = Value.Get(3);
			m2[Value.Get("key22")] = Value.Get(5);

			IDictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("key1")] = Value.Get(m1);
			inputMap[Value.Get("key2")] = Value.Get(m2);

			// Create maps.
			client.Put(args.writePolicy, key, new Bin(binName, inputMap));

			// Set map value to 11 for map key "key21" inside of map key "key2"
			// and retrieve all maps.
			Record record = client.Operate(args.writePolicy, key,
				MapOperation.Put(MapPolicy.Default, binName, Value.Get("key21"), Value.Get(11), CTX.MapKey(Value.Get("key2"))),
				Operation.Get(binName)
				);

			record = client.Get(args.policy, key);
			console.Info("Record: " + record);
		}
	}
}
