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

public sealed class Exists : SyncExample
{
	/// <summary>
	/// Check whether a record exists before and after writing it.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "existskey");
		Bin bin = new("existsbin", "existsvalue");

		bool exists = client.Exists(policy, key);
		console.Info($"Exists before put: {exists}");

		client.Put(writePolicy, key, bin);
		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey}");

		exists = client.Exists(policy, key);
		console.Info($"Exists after put: {exists}");

		client.Delete(writePolicy, key);

		exists = client.Exists(policy, key);
		console.Info($"Exists after delete: {exists}");
	}
}
