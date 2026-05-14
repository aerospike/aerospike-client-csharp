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

public class QueryPage(Console console) : SyncExample(console)
{

	/// <summary>
	/// Query in pages.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		string indexName = "pqidx";
		string binName = "bin";
		string setName = "pq";

		CreateIndex(client, args, setName, indexName, binName);
		WriteRecords(client, args, setName, binName, 190);

		Statement stmt = new();
		stmt.Namespace = args.ns;
		stmt.SetName = setName;
		stmt.BinNames = [binName];
		stmt.Filter = Filter.Range(binName, 1, 200);
		stmt.MaxRecords = 100;

		var filter = PartitionFilter.All();

		// Query 3 pages of records.
		for (int i = 0; i < 3 && !filter.Done; i++)
		{
			console.Info("Query page: " + i);

			using var rs = client.QueryPartitions(null, stmt, filter);

			int count = 0;

			while (rs.Next())
			{
				count++;
			}

			console.Info("Records returned: " + count);
		}
		client.DropIndex(args.policy, args.ns, setName, indexName);

		Key verifyKey = new Key(args.ns, setName, 1);
		Record verifyRecord = client.Get(null, verifyKey);
		if (verifyRecord == null || Convert.ToInt32(verifyRecord.GetValue(binName)) != 1)
		{
			throw new Exception("QueryPage verification failed: expected key 1 in set '" + setName + "' with bin '" + binName + "' = 1.");
		}

		console.Info("QueryPage verified successfully.");
	}

	private void CreateIndex
	(
		IAerospikeClient client,
		Arguments args,
		string setName,
		string indexName,
		string binName
	)
	{
		console.Info($"Create index: ns={args.ns} set={setName} index={indexName} bin={binName}");

		Policy policy = new()
		{
			totalTimeout = 0
		};

		try
		{
			client.DropIndex(policy, args.ns, setName, indexName);
		}
		catch (AerospikeException)
		{
		}

		var task = client.CreateIndex(policy, args.ns, setName, indexName, binName, IndexType.INTEGER);
		task.Wait();
	}

	private void WriteRecords
	(
		IAerospikeClient client,
		Arguments args,
		string setName,
		string binName,
		int size
	)
	{
		console.Info("Write " + size + " records.");

		for (int i = 1; i <= size; i++)
		{
			var key = new Key(args.ns, setName, i);
			var bin = new Bin(binName, i);
			client.Put(args.writePolicy, key, bin);
		}
	}
}
