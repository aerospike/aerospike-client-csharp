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
	public class OperateList : SyncExample
	{
		public OperateList(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Perform operations on a list bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (! args.hasCDTList)
			{
				console.Info("CDT list functions are not supported by the connected Aerospike server.");
				return;
			}	
			RunSimpleExample(client, args);
			RunNestedExample(client, args);
		}

		/// <summary>
		/// Simple example of list functionality.
		/// </summary>
		private void RunSimpleExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "listkey");
			string binName = args.GetBinName("listbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IList inputList = new List<Value>();
			inputList.Add(Value.Get(55));
			inputList.Add(Value.Get(77));

			// Write values to empty list.
			Record record = client.Operate(args.writePolicy, key, ListOperation.AppendItems(binName, inputList));

			console.Info("Record: " + record);

			// Pop value from end of list and also return new size of list.
			record = client.Operate(args.writePolicy, key, ListOperation.Pop(binName, -1), ListOperation.Size(binName));

			console.Info("Record: " + record);

			// There should be one result for each list operation on the same list bin.
			// In this case, there are two list operations (pop and size), so there 
			// should be two results.
			IList list = record.GetList(binName);

			foreach (object value in list)
			{
				console.Info("Received: " + value);
			}
		}

		/// <summary>
		/// Operate on a list of lists.
		/// </summary>
		private void RunNestedExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "listkey2");
			string binName = args.GetBinName("listbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IList<Value> l1 = new List<Value>();
			l1.Add(Value.Get(7));
			l1.Add(Value.Get(9));
			l1.Add(Value.Get(5));

			IList<Value> l2 = new List<Value>();
			l2.Add(Value.Get(1));
			l2.Add(Value.Get(2));
			l2.Add(Value.Get(3));

			IList<Value> l3 = new List<Value>();
			l3.Add(Value.Get(6));
			l3.Add(Value.Get(5));
			l3.Add(Value.Get(4));
			l3.Add(Value.Get(1));

			IList<Value> inputList = new List<Value>();
			inputList.Add(Value.Get(l1));
			inputList.Add(Value.Get(l2));
			inputList.Add(Value.Get(l3));

			// Create list.
			client.Put(args.writePolicy, key, new Bin(binName, inputList));

			// Append value to last list and retrieve all lists.
			Record record = client.Operate(args.writePolicy, key,
				ListOperation.Append(binName, Value.Get(11), CTX.ListIndex(-1)),
				Operation.Get(binName)
				);

			record = client.Get(args.policy, key);
			console.Info("Record: " + record);
		}
	}
}
