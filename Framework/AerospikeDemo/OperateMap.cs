/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
		}

		public void RunSimpleExample(AerospikeClient client, Arguments args)
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

		public void RunScoreExample(AerospikeClient client, Arguments args)
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
	}
}
