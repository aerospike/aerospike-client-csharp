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

public sealed class Expire : SyncExample
{
	/// <summary>
	/// Write a record with a TTL, read it before expiration, and confirm it is gone after expiration.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "expirekey");
		Bin bin = new("expirebin", "expirevalue");

		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={bin.name} value={bin.value} expiration=2");

		WritePolicy expirePolicy = new(writePolicy)
		{
			expiration = 2
		};
		client.Put(expirePolicy, key, bin);

		console.Info($"Get: namespace={key.ns} set={key.setName} key={key.userKey}");
		Record record = client.Get(policy, key, bin.name);
		console.Info($"Record: {record}");

		console.Info("Sleeping for 3 seconds ...");
		Thread.Sleep(TimeSpan.FromSeconds(3));

		record = client.Get(policy, key, bin.name);
		console.Info($"Record after expiration: {record}");
	}
}
