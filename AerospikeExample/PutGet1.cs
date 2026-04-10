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

public class PutGet1(Console console) : SyncExample(console)
{

	/// <summary>
	/// Write and read a bin value.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		if (args.singleBin)
		{
			RunSingleBinTest(client, args);
		}
		else
		{
			RunMultiBinTest(client, args);
		}
		RunGetHeaderTest(client, args);

		var verifyKey = new Key(args.ns, args.set, "putgetkey");
		var verifyRecord = client.Get(null, verifyKey);
		if (verifyRecord == null)
		{
			throw new Exception("PutGet verification failed: record not found for putgetkey.");
		}
		if (args.singleBin)
		{
			object sb = verifyRecord.GetValue("");
			if (sb == null || !sb.Equals("value"))
			{
				throw new Exception($"PutGet verification failed: expected single-bin value \"value\", got {sb}.");
			}
		}
		else
		{
			object v1 = verifyRecord.GetValue("bin1");
			object v2 = verifyRecord.GetValue("bin2");
			if (v1 == null || !v1.Equals("value1") || v2 == null || !v2.Equals("value2"))
			{
				throw new Exception($"PutGet verification failed: expected bin1=value1, bin2=value2; got bin1={v1}, bin2={v2}.");
			}
		}
		console.Info("PutGet verified successfully.");
	}

	/// <summary>
	/// Execute put and get on a server configured as multi-bin.  This is the server default.
	/// </summary>
	private void RunMultiBinTest(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "putgetkey");
		var bin1 = new Bin("bin1", "value1");
		var bin2 = new Bin("bin2", "value2");

		console.Info("Put: namespace={0} set={1} key={2} bin1={3} value1={4} bin2={5} value2={6}",
			key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

		// test comment
		var test = 12345;

		client.Put(args.writePolicy, key, bin1, bin2);

		console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

		var record = client.Get(args.policy, key);

		if (record == null)
		{
			throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		}

		ValidateBin(key, bin1, record);
		ValidateBin(key, bin2, record);
	}

	/// <summary>
	/// Execute put and get on a server configured as single-bin.
	/// </summary>
	private void RunSingleBinTest(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "putgetkey");
		var bin = new Bin("", "value");

		console.Info("Single Bin Put: namespace={0} set={1} key={2} value={3}",
			key.ns, key.setName, key.userKey, bin.value);

		client.Put(args.writePolicy, key, bin);

		console.Info("Single Bin Get: namespace={0} set={1} key={2}", key.ns,
			key.setName, key.userKey);

		var record = client.Get(args.policy, key);

		if (record == null)
		{
			throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		}

		ValidateBin(key, bin, record);
	}

	private void ValidateBin(Key key, Bin bin, Record record)
	{
		object received = record.GetValue(bin.name);
		string expected = bin.value.ToString();

		if (received != null && received.Equals(expected))
		{
			console.Info("Bin matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}",
				key.ns, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
		}
		else
		{
			console.Error("Put/Get mismatch: Expected {0}. Received {1}.", expected, received);
		}
	}

	/// <summary>
	/// Read record header data.
	/// </summary>
	private void RunGetHeaderTest(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "putgetkey");

		console.Info("Get record header: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);
		var record = client.GetHeader(args.policy, key);

		if (record == null)
		{
			throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		}

		// Generation should be greater than zero.  Make sure it's populated.
		if (record.generation == 0)
		{
			throw new Exception($"Invalid record header: generation={record.generation:D} expiration={record.expiration:D}");
		}
		console.Info("Received: generation={0} expiration={1}", record.generation, record.expiration);
	}
}
