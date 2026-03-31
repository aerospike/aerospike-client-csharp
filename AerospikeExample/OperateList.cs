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

namespace Aerospike.Example
{
	public class OperateList(Console console) : SyncExample(console)
	{

		/// <summary>
		/// Perform operations on a list bin.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			RunSimpleExample(client, args);
			RunNestedExample(client, args);

			string listBinName = args.GetBinName("listbin");
			Record listVerify = client.Get(null, new Key(args.ns, args.set, "listkey"));
			if (listVerify == null || listVerify.GetValue(listBinName) == null)
			{
				throw new Exception("OperateList verification failed: listkey or list bin missing.");
			}
			console.Info("OperateList verified successfully.");
		}

		/// <summary>
		/// Simple example of list functionality.
		/// </summary>
		private void RunSimpleExample(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "listkey");
			string binName = args.GetBinName("listbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IList inputList = new List<Value>
			{
				Value.Get(55),
				Value.Get(77)
			};

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
		private void RunNestedExample(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "listkey2");
			string binName = args.GetBinName("listbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			IList<Value> l1 = [Value.Get(7), Value.Get(9), Value.Get(5)];

			IList<Value> l2 = [Value.Get(1), Value.Get(2), Value.Get(3)];

			IList<Value> l3 = [Value.Get(6), Value.Get(5), Value.Get(4), Value.Get(1)];

			IList<Value> inputList = [Value.Get(l1), Value.Get(l2), Value.Get(l3)];

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
