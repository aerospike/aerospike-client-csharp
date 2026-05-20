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

public sealed class DeleteBin : SyncExample
{
	/// <summary>
	/// Drop a single bin from a record by writing a null bin value.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "delbinkey");
		string binName1 = "bin1";
		string binName2 = "bin2";

		console.Info("Delete one bin in the record.");
		Bin nullBin = Bin.AsNull(binName1); // Set bin value to null to drop bin.
		client.Put(writePolicy, key, nullBin);

		console.Info("Read record.");
		Record record = client.Get(policy, key, nullBin.name, binName2, "bin3");

		if (record == null)
		{
			return;
		}

		foreach (KeyValuePair<string, object> entry in record.bins)
		{
			console.Info($"Received: namespace={key.ns} set={key.setName} key={key.userKey} bin={entry.Key} value={entry.Value}");
		}
	}
}
