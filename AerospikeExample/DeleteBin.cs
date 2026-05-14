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

public class DeleteBin(Console console) : SyncExample(console)
{

	/// <summary>
	/// Drop a bin from a record.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		if (args.singleBin)
		{
			console.Info("Delete bin is not applicable to single bin servers.");
			return;
		}

		console.Info("Write multi-bin record.");
		var key = new Key(args.ns, args.set, "delbinkey");
		string binName1 = args.GetBinName("bin1");
		string binName2 = args.GetBinName("bin2");
		var bin1 = new Bin(binName1, "value1");
		var bin2 = new Bin(binName2, "value2");
		client.Put(args.writePolicy, key, bin1, bin2);

		console.Info("Delete one bin in the record.");
		bin1 = Bin.AsNull(binName1); // Set bin value to null to drop bin.
		client.Put(args.writePolicy, key, bin1);

		console.Info("Read record.");
		var record = client.Get(args.policy, key, bin1.name, bin2.name, "bin3");

		if (record == null)
		{
			throw new Exception($"Failed to get: namespace={key.ns} set={key.setName} key={key.userKey}");
		}

		foreach (KeyValuePair<string, object> entry in record.bins)
		{
			console.Info("Received: namespace={0} set={1} key={2} bin={3} value={4}",
				key.ns, key.setName, key.userKey, entry.Key, entry.Value);
		}

		bool valid = true;

		if (record.GetValue("bin1") != null)
		{
			console.Error("bin1 still exists.");
			valid = false;
		}

		object v2 = record.GetValue("bin2");

		if (v2 == null || !v2.Equals("value2"))
		{
			console.Error("bin2 value mismatch.");
			valid = false;
		}

		if (valid)
		{
			console.Info("Bin delete successful");
		}

		string verifyBinName1 = args.GetBinName("bin1");
		string verifyBinName2 = args.GetBinName("bin2");
		var verifyKey = new Key(args.ns, args.set, "delbinkey");
		var verifyRecord = client.Get(null, verifyKey);
		if (verifyRecord == null)
		{
			throw new Exception("DeleteBin verification failed: delbinkey record not found.");
		}
		if (verifyRecord.GetValue(verifyBinName1) != null)
		{
			throw new Exception("DeleteBin verification failed: bin1 should be absent.");
		}
		object dv2 = verifyRecord.GetValue(verifyBinName2);
		if (dv2 == null || !dv2.Equals("value2"))
		{
			throw new Exception($"DeleteBin verification failed: expected bin2=value2, got {dv2}.");
		}
		console.Info("DeleteBin verified successfully.");
	}
}
