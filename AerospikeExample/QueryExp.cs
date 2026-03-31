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

public class QueryExp(Console console) : SyncExample(console)
{

	/// <summary>
	/// Perform secondary index queries with predicate filters.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		string indexName = "predidx";
		string binName = "idxbin";
		int size = 50;

		CreateIndex(client, args, indexName, binName);
		WriteRecords(client, args, binName, size);
		RunQuery1(client, args, binName);
		RunQuery2(client, args, binName);
		RunQuery3(client, args, binName);
		client.DropIndex(args.policy, args.ns, args.set, indexName);

		var verifyKey = new Key(args.ns, args.set, 1);
		Record verifyRec = client.Get(null, verifyKey) ?? throw new Exception("QueryExp verification: record key 1 not found.");
		if (!verifyRec.bins.ContainsKey(binName))
		{
			throw new Exception("QueryExp verification: idxbin missing.");
		}
		console.Info("QueryExp verified successfully.");
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

		var task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.INTEGER);
		task.Wait();
	}

	private void WriteRecords(IAerospikeClient client, Arguments args, string binName, int size)
	{
		console.Info("Write " + size + " records.");

		for (int i = 1; i <= size; i++)
		{
			var key = new Key(args.ns, args.set, i);
			var bin1 = new Bin(binName, i);
			var bin2 = new Bin("bin2", i * 10);
			var bin3 = i % 4 == 0
				? new Bin("bin3", "prefix-" + i + "-suffix")
				: i % 2 == 0
					? new Bin("bin3", "prefix-" + i + "-SUFFIX")
					: new Bin("bin3", "pre-" + i + "-suf");
			client.Put(args.writePolicy, key, bin1, bin2, bin3);
		}
	}

	private void RunQuery1(IAerospikeClient client, Arguments args, string binName)
	{
		int begin = 10;
		int end = 40;

		console.Info("Query Predicate: (bin2 > 126 && bin2 <= 140) || (bin2 = 360)");

		Statement stmt = new();
		stmt.SetNamespace(args.ns);
		stmt.SetSetName(args.set);

		// Filter applied on query itself.  Filter can only reference an indexed bin.
		stmt.SetFilter(Filter.Range(binName, begin, end));

		// Predicates are applied on query results on server side.
		// Predicates can reference any bin.
		QueryPolicy policy = new(client.QueryPolicyDefault);
		policy.filterExp = Exp.Build(
			Exp.Or(
				Exp.And(
					Exp.GT(Exp.IntBin("bin2"), Exp.Val(126)),
					Exp.LE(Exp.IntBin("bin2"), Exp.Val(140))),
				Exp.EQ(Exp.IntBin("bin2"), Exp.Val(360))));

		using var rs = client.Query(policy, stmt);

		while (rs.Next())
		{
			var record = rs.Record;
			console.Info("Record: " + record.ToString());
		}
	}

	private void RunQuery2(IAerospikeClient client, Arguments args, string binName)
	{
		int begin = 10;
		int end = 40;

		console.Info("Query Predicate: Record updated in 2020");
		var beginTime = new DateTime(2020, 1, 1);
		var endTime = new DateTime(2021, 1, 1);

		Statement stmt = new();
		stmt.SetNamespace(args.ns);
		stmt.SetSetName(args.set);
		stmt.SetFilter(Filter.Range(binName, begin, end));

		QueryPolicy policy = new(client.QueryPolicyDefault);
		policy.filterExp = Exp.Build(
			Exp.And(
				Exp.GE(Exp.LastUpdate(), Exp.Val(beginTime)),
				Exp.LT(Exp.LastUpdate(), Exp.Val(endTime))));

		using var rs = client.Query(policy, stmt);

		while (rs.Next())
		{
			var record = rs.Record;
			console.Info("Record: " + record.ToString());
		}
	}

	private void RunQuery3(IAerospikeClient client, Arguments args, string binName)
	{
		int begin = 20;
		int end = 30;

		console.Info("Query Predicate: bin3 contains string with 'prefix' and 'suffix'");

		Statement stmt = new();
		stmt.SetNamespace(args.ns);
		stmt.SetSetName(args.set);
		stmt.SetFilter(Filter.Range(binName, begin, end));

		QueryPolicy policy = new(client.QueryPolicyDefault);
		policy.filterExp = Exp.Build(
			Exp.RegexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.StringBin("bin3")));

		using var rs = client.Query(policy, stmt);

		while (rs.Next())
		{
			var record = rs.Record;
			console.Info("Record: " + record.ToString());
		}
	}
}
