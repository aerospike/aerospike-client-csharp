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

public sealed class Generation : SyncExample
{
	/// <summary>
	/// Demonstrate optimistic concurrency control using record generations.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "genkey");
		string binName = "genbin";

		Bin first = new(binName, "genvalue1");
		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={binName} value={first.value}");
		client.Put(writePolicy, key, first);

		Bin second = new(binName, "genvalue2");
		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={binName} value={second.value}");
		client.Put(writePolicy, key, second);

		Record record = client.Get(policy, key, binName);
		console.Info($"Get: namespace={key.ns} set={key.setName} key={key.userKey} bin={binName} value={record?.GetValue(binName)} generation={record?.generation}");

		Bin third = new(binName, "genvalue3");
		int expectedGeneration = record?.generation ?? 0;
		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={binName} value={third.value} expected generation={expectedGeneration}");

		WritePolicy expectGenPolicy = new(writePolicy)
		{
			generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
			generation = expectedGeneration
		};
		client.Put(expectGenPolicy, key, third);

		Bin fourth = new(binName, "genvalue4");
		WritePolicy invalidGenPolicy = new(expectGenPolicy)
		{
			generation = 9999
		};
		console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={binName} value={fourth.value} expected generation={invalidGenPolicy.generation}");

		try
		{
			client.Put(invalidGenPolicy, key, fourth);
			console.Info("Put with invalid generation completed unexpectedly.");
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.GENERATION_ERROR)
		{
			console.Info("Success: Generation error returned as expected.");
		}
	}
}
