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

public sealed class Touch : SyncExample
{
	/// <summary>
	/// Demonstrate the touch command extending an existing record's TTL.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "touchkey");
		Bin bin = new("touchbin", "touchvalue");

		Console.WriteLine("Create record with 2 second expiration.");
		WritePolicy initialPolicy = new(writePolicy)
		{
			expiration = 2
		};
		client.Put(initialPolicy, key, bin);

		Console.WriteLine("Touch same record with 5 second expiration.");
		WritePolicy touchPolicy = new(writePolicy)
		{
			expiration = 5
		};
		Record touched = client.Operate(touchPolicy, key, Operation.Touch(), Operation.GetHeader());
		Console.WriteLine($"Header: generation={touched?.generation} expiration={touched?.expiration}");

		Console.WriteLine("Sleep 3 seconds.");
		Thread.Sleep(TimeSpan.FromSeconds(3));

		Record record = client.Get(policy, key, bin.name);
		Console.WriteLine($"Record after 3 seconds: {record}");

		Console.WriteLine("Sleep 4 seconds.");
		Thread.Sleep(TimeSpan.FromSeconds(4));

		record = client.Get(policy, key, bin.name);
		Console.WriteLine($"Record after expiration: {record}");
	}
}
