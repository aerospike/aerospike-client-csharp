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

public sealed class Add : SyncExample
{
	/// <summary>
	/// Add integer values to a record bin.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "addkey");
		string binName = "addbin";

		Bin initial = new(binName, 10);
		console.Info($"Initial add will create record. Initial value is {initial.value}.");
		client.Add(writePolicy, key, initial);

		Bin increment = new(binName, 5);
		console.Info($"Add {increment.value} to existing record.");
		client.Add(writePolicy, key, increment);

		Bin combined = new(binName, 30);
		console.Info($"Add {combined.value} to existing record.");
		Record record = client.Operate(writePolicy, key, Operation.Add(combined), Operation.Get(combined.name));
		console.Info($"Add result: namespace={key.ns} set={key.setName} key={key.userKey} bin={combined.name} value={record.GetInt(combined.name)}");
	}
}
