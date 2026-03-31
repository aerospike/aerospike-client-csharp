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

public class Replace(Console console) : SyncExample(console)
{

	/// <summary>
	/// Demonstrate writing bins with replace option. Replace will cause all record bins
	/// to be overwritten.  If an existing bin is not referenced in the replace command,
	/// the bin will be deleted.
	/// <para>
	/// The replace command has a performance advantage over the default put, because 
	/// the server does not have to read the existing record before overwriting it.
	/// </para>
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RunReplaceExample(client, args);
		RunReplaceOnlyExample(client, args);

		var verifyKey = new Key(args.ns, args.set, "replacekey");
		var verifyRecord = client.Get(null, verifyKey);
		if (verifyRecord == null)
		{
			throw new Exception("Replace verification failed: replacekey record not found.");
		}
		if (verifyRecord.GetValue("bin1") != null)
		{
			throw new Exception("Replace verification failed: bin1 should be absent.");
		}
		if (verifyRecord.GetValue("bin2") != null)
		{
			throw new Exception("Replace verification failed: bin2 should be absent.");
		}
		object v3 = verifyRecord.GetValue("bin3");
		if (v3 == null || !v3.Equals("value3"))
		{
			throw new Exception($"Replace verification failed: expected bin3=value3, got {v3}.");
		}
		console.Info("Replace verified successfully.");
	}

	private void RunReplaceExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "replacekey");
		var bin1 = new Bin("bin1", "value1");
		var bin2 = new Bin("bin2", "value2");
		var bin3 = new Bin("bin3", "value3");

		console.Info("Put: namespace={0} set={1} key={2} bin1={3} value1={4} bin2={5} value2={6}",
			key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

		client.Put(args.writePolicy, key, bin1, bin2);

		console.Info("Replace with: namespace={0} set={1} key={2} bin={3} value={4}",
			key.ns, key.setName, key.userKey, bin3.name, bin3.value);

		WritePolicy policy = new()
		{
			recordExistsAction = RecordExistsAction.REPLACE
		};
		client.Put(policy, key, bin3);

		console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

		var record = client.Get(args.policy, key) ?? throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		if (record.GetValue(bin1.name) == null)
		{
			console.Info(bin1.name + " was deleted as expected.");
		}
		else
		{
			console.Error(bin1.name + " found when it should have been deleted.");
		}

		if (record.GetValue(bin2.name) == null)
		{
			console.Info(bin2.name + " was deleted as expected.");
		}
		else
		{
			console.Error(bin2.name + " found when it should have been deleted.");
		}
		ValidateBin(key, bin3, record);
	}

	private void RunReplaceOnlyExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "replaceonlykey");
		var bin = new Bin("bin", "value");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

		console.Info("Replace record requiring that it exists: namespace={0} set={1} key={2}",
			key.ns, key.setName, key.userKey);

		try
		{
			WritePolicy policy = new();
			policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
			client.Put(policy, key, bin);

			console.Error("Failure. This command should have resulted in an error.");
		}
		catch (AerospikeException ae)
		{
			if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				console.Info("Success. Key not found error returned as expected.");
			}
			else
			{
				throw;
			}
		}
	}

	private void ValidateBin(Key key, Bin bin, Record record)
	{
		object received = record.GetValue(bin.name);
		string expected = bin.value.ToString();

		if (received != null && received.Equals(expected))
		{
			console.Info("Data matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}",
				key.ns, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
		}
		else
		{
			console.Error("Data mismatch: Expected {0}. Received {1}", expected, received);
		}
	}
}
