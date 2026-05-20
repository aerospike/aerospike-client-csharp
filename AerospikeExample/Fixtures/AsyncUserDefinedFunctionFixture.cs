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

internal static class AsyncUserDefinedFunctionFixture
{
	public static void Setup(IAerospikeClient client, Arguments args)
	{
		Cleanup(client, args);
		LuaExample.Register(client, args.policy, "record_example.lua");
	}

	public static void Validate(IAerospikeClient client, Arguments args)
	{
		Key key = new(args.ns, args.set, "audfkey1");
		string binName = "audfbin1";
		Record record = client.Get(null, key, binName);
		string received = (string)record?.GetValue(binName);

		if (received == null || !received.Equals("string value"))
		{
			throw new Exception($"AsyncUserDefinedFunction verification failed: expected \"string value\", received \"{received}\".");
		}
	}

	public static void Cleanup(IAerospikeClient client, Arguments args)
	{
		client.Delete(args.writePolicy, new Key(args.ns, args.set, "audfkey1"));
	}
}
