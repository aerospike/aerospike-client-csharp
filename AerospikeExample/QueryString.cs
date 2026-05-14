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

public class QueryString(Console console) : SyncExample(console)
{

	/// <summary>
	/// Create secondary index and query on it.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		string indexName = "queryindex";
		string keyPrefix = "querykey";
		string valuePrefix = "queryvalue";
		string binName = args.GetBinName("querybin");
		int size = 5;

		CreateIndex(client, args, indexName, binName);
		WriteRecords(client, args, keyPrefix, binName, valuePrefix, size);
		RunQuery(client, args, indexName, binName, valuePrefix);
		client.DropIndex(args.policy, args.ns, args.set, indexName);

		var verifyKey = new Key(args.ns, args.set, "querykey3");
		Record verifyRec = client.Get(null, verifyKey) ?? throw new Exception("QueryString verification: record querykey3 not found.");
		string verifyBin = args.GetBinName("querybin");
		object verifyVal = verifyRec.GetValue(verifyBin);
		if (!"queryvalue3".Equals(verifyVal))
		{
			throw new Exception($"QueryString verification: expected querybin queryvalue3, got {verifyVal}.");
		}
		console.Info("QueryString verified successfully.");
	}

	private void CreateIndex(IAerospikeClient client, Arguments args, string indexName, string binName)
	{
		console.Info($"Create index: ns={args.ns} set={args.set} index={indexName} bin={binName}");

		Policy policy = new()
		{
			totalTimeout = 0 // Do not timeout on index create.
		};

		try
		{
			client.DropIndex(policy, args.ns, args.set, indexName);
		}
		catch (AerospikeException)
		{
		}

		var task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING);
		task.Wait();
	}

	private void WriteRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName, string valuePrefix, int size)
	{
		for (int i = 1; i <= size; i++)
		{
			var key = new Key(args.ns, args.set, keyPrefix + i);
			var bin = new Bin(binName, valuePrefix + i);

			console.Info($"Put: namespace={key.ns} set={key.setName} key={key.userKey} bin={bin.name} value={bin.value}");

			client.Put(args.writePolicy, key, bin);
		}
	}

	private void RunQuery(IAerospikeClient client, Arguments args, string indexName, string binName, string valuePrefix)
	{
		string filter = valuePrefix + 3;

		console.Info($"Query for: ns={args.ns} set={args.set} index={indexName} bin={binName} filter={filter}");

		Statement stmt = new();
		stmt.SetNamespace(args.ns);
		stmt.SetSetName(args.set);
		stmt.SetBinNames(binName);
		stmt.SetFilter(Filter.Equal(binName, filter));

		using var rs = client.Query(null, stmt);

		int count = 0;

		while (rs.Next())
		{
			var key = rs.Key;
			var record = rs.Record;
			string result = (string)record.GetValue(binName);

			if (result.Equals(filter))
			{
				console.Info($"Record found: namespace={key.ns} set={key.setName} digest={ByteUtil.BytesToHexString(key.digest)} bin={binName} value={result}");
			}
			else
			{
				console.Error($"Query mismatch: Expected {filter}. Received {result}.");
			}
			count++;
		}

		if (count == 0)
		{
			console.Error("Query failed. No records returned.");
		}
	}
}
