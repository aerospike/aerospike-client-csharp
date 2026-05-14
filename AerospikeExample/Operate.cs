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

public class Operate(Console console) : SyncExample(console)
{

	/// <summary>
	/// Demonstrate multiple operations on a single record in one call.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		// Write initial record.
		var key = new Key(args.ns, args.set, "opkey");
		var bin1 = new Bin("optintbin", 7);
		var bin2 = new Bin("optstringbin", "string value");
		console.Info("Put: namespace={0} set={1} key={2} binname1={3} binvalue1={4} binname1={5} binvalue1={6}",
			key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
		client.Put(args.writePolicy, key, bin1, bin2);

		// Add integer, write new string and read record.
		var bin3 = new Bin(bin1.name, 4);
		var bin4 = new Bin(bin2.name, "new string");
		console.Info("Add: " + bin3.value);
		console.Info("Write: " + bin4.value);
		console.Info("Read:");
		var record = client.Operate(args.writePolicy, key, Operation.Add(bin3), Operation.Put(bin4), Operation.Get());

		if (record == null)
		{
			throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		}

		ValidateBin(key, record, bin3.name, 11L, record.GetValue(bin3.name));
		ValidateBin(key, record, bin4.name, bin4.value.ToString(), record.GetValue(bin4.name));

		var verifyKey = new Key(args.ns, args.set, "opkey");
		var verifyRecord = client.Get(null, verifyKey);
		if (verifyRecord == null)
		{
			throw new Exception("Operate verification failed: opkey record not found.");
		}
		object optIntVal = verifyRecord.GetValue("optintbin");
		if (verifyRecord.GetLong("optintbin") != 11L)
		{
			throw new Exception($"Operate verification failed: expected optintbin=11, got {optIntVal}.");
		}
		object os = verifyRecord.GetValue("optstringbin");
		if (os == null || !os.Equals("new string"))
		{
			throw new Exception($"Operate verification failed: expected optstringbin=\"new string\", got {os}.");
		}
		console.Info("Operate verified successfully.");
	}

	private void ValidateBin(Key key, Record record, string binName, object expected, object received)
	{
		if (received != null && received.Equals(expected))
		{
			console.Info("Bin matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}",
				key.ns, key.setName, key.userKey, binName, received, record.generation, record.expiration);
		}
		else
		{
			console.Error("Bin mismatch: Expected {0}. Received {1}.", expected, received);
		}
	}
}
