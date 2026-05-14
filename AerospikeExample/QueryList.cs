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
using System.Collections;

namespace Aerospike.Example;

public class QueryList(Console console) : SyncExample(console)
{

	/// <summary>
	/// Create secondary index and query on list bins.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		string indexName = "qlindex";
		string keyPrefix = "qlkey";
		string binName = "listbin";
		int size = 20;

		CreateIndex(client, args, indexName, binName);
		WriteRecords(client, args, keyPrefix, binName, size);
		RunQuery(client, args, indexName, binName);
		client.DropIndex(args.policy, args.ns, args.set, indexName);

		var verifyKey = new Key(args.ns, args.set, "qlkey1");
		Record verifyRec = client.Get(null, verifyKey) ?? throw new Exception("QueryList verification: record qlkey1 not found.");
		if (verifyRec.GetList(binName) == null)
		{
			throw new Exception("QueryList verification: listbin is null or not a list.");
		}
		console.Info("QueryList verified successfully.");
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

		var task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING, IndexCollectionType.LIST);
		task.Wait();
	}

	private void WriteRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
	{
		console.Info("Write records");
		var random = new Random();

		for (int i = 1; i <= size; i++)
		{
			var key = new Key(args.ns, args.set, keyPrefix + i);

			List<string> list =
			[
				random.Next(900, 910).ToString(),
				random.Next(900, 910).ToString(),
				random.Next(900, 910).ToString(),
			];

			var bin = new Bin(binName, list);

			client.Put(args.writePolicy, key, bin);
		}
	}

	private void RunQuery(IAerospikeClient client, Arguments args, string indexName, string binName)
	{
		console.Info("Query list bins");

		Statement stmt = new();
		stmt.SetNamespace(args.ns);
		stmt.SetSetName(args.set);
		stmt.SetBinNames(binName);
		stmt.SetFilter(Filter.Contains(binName, IndexCollectionType.LIST, "905"));

		using var rs = client.Query(null, stmt);

		int count = 0;

		while (rs.Next())
		{
			count++;
			var key = rs.Key;
			var record = rs.Record;
			IList list = record.GetList(binName);

			console.Info("Record " + count);

			foreach (string s in list)
			{
				console.Info(s);
			}
		}
	}
}
