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

internal static class UserDefinedFunctionFixture
{
	private static readonly string[] UserKeys =
		["udfkey1", "udfkey2", "udfkey3", "udfkey4", "udfkey5", "udfkey7"];

	private static readonly List<object> ExpectedList =
	[
		"string1",
		4L,
		new List<object> { "string2", 8L },
		new Dictionary<object, object>
		{
			["a"] = 1L,
			[2L] = "b",
			["list"] = new List<object> { "string2", 8L }
		}
	];

	public static void Setup(IAerospikeClient client, Arguments args)
	{
		Cleanup(client, args);
		LuaExample.Register(client, args.policy, "record_example.lua");
		ExampleFixtureSupport.PutBins(client, args, "udfkey2", new Bin("udfbin2", "string value"));
		ExampleFixtureSupport.PutBins(client, args, "udfkey7", new Bin("udfbin7", new List<int> { 64, 3702, -5 }));
	}

	public static void Validate(IAerospikeClient client, Arguments args)
	{
		ExampleFixtureSupport.AssertBin(client, args, "udfkey1", "udfbin1", "string value");
		ExampleFixtureSupport.AssertBin(client, args, "udfkey2", "udfbin2", "string value");
		ExampleFixtureSupport.AssertBin(client, args, "udfkey3", "udfbin3", "first");
		ExampleFixtureSupport.AssertBin(client, args, "udfkey4", "udfbin4", 4L);

		object received = client.Execute(
			args.writePolicy,
			ExampleFixtureSupport.Key(args, "udfkey5"),
			"record_example",
			"readBin",
			Value.Get("udfbin5"));

		if (!ExampleFixtureSupport.CdtEquals(received, ExpectedList))
		{
			throw new Exception("UserDefinedFunction verification failed: list/map value did not match.");
		}

		long found = (long)client.Execute(
			args.writePolicy,
			ExampleFixtureSupport.Key(args, "udfkey7"),
			"record_example",
			"valueExists",
			Value.Get("udfbin7"),
			Value.Get(3702));

		long missing = (long)client.Execute(
			args.writePolicy,
			ExampleFixtureSupport.Key(args, "udfkey7"),
			"record_example",
			"valueExists",
			Value.Get("udfbin7"),
			Value.Get(65));

		if (found == 0 || missing != 0)
		{
			throw new Exception("UserDefinedFunction verification failed: server-side exists returned unexpected values.");
		}
	}

	public static void Cleanup(IAerospikeClient client, Arguments args)
		=> ExampleFixtureSupport.DeleteKeys(client, args, UserKeys);
}
