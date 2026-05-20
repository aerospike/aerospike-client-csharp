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

public sealed class OperateList : SyncExample
{
	/// <summary>
	/// Perform operations on list bins.
	/// </summary>
	public override void RunExample()
	{
		RunSimpleExample();
		RunNestedExample();
	}

	/// <summary>
	/// Append, pop, and size on a simple list bin.
	/// </summary>
	private void RunSimpleExample()
	{
		Key key = new(ns, set, "listkey");
		string binName = "listbin";

		IList inputList = new List<Value>
		{
			Value.Get(55),
			Value.Get(77)
		};

		Record record = client.Operate(writePolicy, key, ListOperation.AppendItems(binName, inputList));
		console.Info($"Record: {record}");

		// Pop value from end of list and return new size of list.
		record = client.Operate(writePolicy, key,
			ListOperation.Pop(binName, -1),
			ListOperation.Size(binName));
		console.Info($"Record: {record}");

		// Two operations on the same bin produce a list of two results.
		IList results = record.GetList(binName);

		foreach (object value in results)
		{
			console.Info($"Received: {value}");
		}
	}

	/// <summary>
	/// Operate on a list of lists by navigating with a context.
	/// </summary>
	private void RunNestedExample()
	{
		Key key = new(ns, set, "listkey2");
		string binName = "listbin";

		IList<Value> l1 = [Value.Get(7), Value.Get(9), Value.Get(5)];
		IList<Value> l2 = [Value.Get(1), Value.Get(2), Value.Get(3)];
		IList<Value> l3 = [Value.Get(6), Value.Get(5), Value.Get(4), Value.Get(1)];
		IList<Value> inputList = [Value.Get(l1), Value.Get(l2), Value.Get(l3)];

		client.Put(writePolicy, key, new Bin(binName, inputList));

		// Append value to the last list and retrieve all lists.
		client.Operate(writePolicy, key,
			ListOperation.Append(binName, Value.Get(11), CTX.ListIndex(-1)),
			Operation.Get(binName));

		Record record = client.Get(policy, key);
		console.Info($"Record: {record}");
	}
}
