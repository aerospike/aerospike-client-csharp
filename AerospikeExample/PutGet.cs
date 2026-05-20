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

public sealed class PutGet : SyncExample
{
	/// <summary>
	/// Write a bin value and read it back.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "putgetkey");
		Bin bin = new("bin1", "value1");

		client.Put(writePolicy, key, bin);

		Record record = client.Get(policy, key);
		console.Info($"Record: {record}");

		Record header = client.GetHeader(policy, key);
		console.Info($"Header: generation={header.generation} expiration={header.expiration}");
	}
}
