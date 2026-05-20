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

public sealed class UserDefinedFunction : SyncExample
{
	private const string Package = "record_example";

	/// <summary>
	/// Invoke pre-registered Lua user-defined functions from the client.
	/// </summary>
	public override void RunExample()
	{
		WriteUsingUdf();
		WriteIfGenerationNotChanged();
		WriteIfNotExists();
		WriteWithValidation();
		WriteListMapUsingUdf();
		ServerSideExists();
	}

	private void WriteUsingUdf()
	{
		Key key = new(ns, set, "udfkey1");
		Bin bin = new("udfbin1", "string value");

		client.Execute(writePolicy, key, Package, "writeBin", Value.Get(bin.name), bin.value);
	}

	private void WriteIfGenerationNotChanged()
	{
		Key key = new(ns, set, "udfkey2");
		Bin bin = new("udfbin2", "string value");

		long gen = (long)client.Execute(writePolicy, key, Package, "getGeneration");
		client.Execute(writePolicy, key, Package, "writeIfGenerationNotChanged",
			Value.Get(bin.name), bin.value, Value.Get(gen));
	}

	private void WriteIfNotExists()
	{
		Key key = new(ns, set, "udfkey3");
		string binName = "udfbin3";

		// First write should succeed (record does not exist yet).
		client.Execute(writePolicy, key, Package, "writeUnique", Value.Get(binName), Value.Get("first"));

		// Second write should be silently ignored by the Lua function.
		client.Execute(writePolicy, key, Package, "writeUnique", Value.Get(binName), Value.Get("second"));
	}

	private void WriteWithValidation()
	{
		Key key = new(ns, set, "udfkey4");
		string binName = "udfbin4";

		// writeWithValidation accepts values between 1 and 10.
		client.Execute(writePolicy, key, Package, "writeWithValidation", Value.Get(binName), Value.Get(4));

		try
		{
			client.Execute(writePolicy, key, Package, "writeWithValidation", Value.Get(binName), Value.Get(11));
		}
		catch (AerospikeException)
		{
			console.Info("UDF rejected the invalid value as expected.");
		}
	}

	private void WriteListMapUsingUdf()
	{
		Key key = new(ns, set, "udfkey5");

		List<object> inner = ["string2", 8L];

		Dictionary<object, object> innerMap = new()
		{
			["a"] = 1L,
			[2L] = "b",
			["list"] = inner
		};

		List<object> list = ["string1", 4L, inner, innerMap];

		string binName = "udfbin5";
		client.Execute(writePolicy, key, Package, "writeBin", Value.Get(binName), Value.Get(list));

		object received = client.Execute(writePolicy, key, Package, "readBin", Value.Get(binName));
		console.Info($"UDF returned: {received}");
	}

	private void ServerSideExists()
	{
		Key key = new(ns, set, "udfkey7");
		string binName = "udfbin7";

		long exists = (long)client.Execute(writePolicy, key, Package, "valueExists", Value.Get(binName), Value.Get(3702));
		console.Info($"Value exists: {exists != 0}");
	}
}
