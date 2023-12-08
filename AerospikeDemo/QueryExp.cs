/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System;

namespace Aerospike.Demo
{
	public class QueryExp : SyncExample
	{
		public QueryExp(Console console)
			: base(console)
		{
		}

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
		}

		private void CreateIndex(IAerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}
		}

		private void WriteRecords(IAerospikeClient client, Arguments args, string binName, int size)
		{
			console.Info("Write " + size + " records.");

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, i);
				Bin bin1 = new Bin(binName, i);
				Bin bin2 = new Bin("bin2", i * 10);
				Bin bin3;

				if (i % 4 == 0)
				{
					bin3 = new Bin("bin3", "prefix-" + i + "-suffix");
				}
				else if (i % 2 == 0)
				{
					bin3 = new Bin("bin3", "prefix-" + i + "-SUFFIX");
				}
				else
				{
					bin3 = new Bin("bin3", "pre-" + i + "-suf");
				}
				client.Put(args.writePolicy, key, bin1, bin2, bin3);
			}
		}

		private void RunQuery1(IAerospikeClient client, Arguments args, string binName)
		{
			int begin = 10;
			int end = 40;

			console.Info("Query Predicate: (bin2 > 126 && bin2 <= 140) || (bin2 = 360)");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);

			// Filter applied on query itself.  Filter can only reference an indexed bin.
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Predicates are applied on query results on server side.
			// Predicates can reference any bin.
			QueryPolicy policy = new QueryPolicy(client.QueryPolicyDefault);
			policy.filterExp = Exp.Build(
				Exp.Or(
					Exp.And(
						Exp.GT(Exp.IntBin("bin2"), Exp.Val(126)),
						Exp.LE(Exp.IntBin("bin2"), Exp.Val(140))),
					Exp.EQ(Exp.IntBin("bin2"), Exp.Val(360))));

			RecordSet rs = client.Query(policy, stmt);

			try
			{
				while (rs.Next())
				{
					Record record = rs.Record;
					console.Info("Record: " + record.ToString());
				}
			}
			finally
			{
				rs.Close();
			}
		}

		private void RunQuery2(IAerospikeClient client, Arguments args, string binName)
		{
			int begin = 10;
			int end = 40;

			console.Info("Query Predicate: Record updated in 2020");
			DateTime beginTime = new DateTime(2020, 1, 1);
			DateTime endTime = new DateTime(2021, 1, 1);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new QueryPolicy(client.QueryPolicyDefault);
			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.GE(Exp.LastUpdate(), Exp.Val(beginTime)),
					Exp.LT(Exp.LastUpdate(), Exp.Val(endTime))));

			RecordSet rs = client.Query(policy, stmt);

			try
			{
				while (rs.Next())
				{
					Record record = rs.Record;
					console.Info("Record: " + record.ToString());
				}
			}
			finally
			{
				rs.Close();
			}
		}

		private void RunQuery3(IAerospikeClient client, Arguments args, string binName)
		{
			int begin = 20;
			int end = 30;

			console.Info("Query Predicate: bin3 contains string with 'prefix' and 'suffix'");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			QueryPolicy policy = new QueryPolicy(client.QueryPolicyDefault);
			policy.filterExp = Exp.Build(
				Exp.RegexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.StringBin("bin3")));

			RecordSet rs = client.Query(policy, stmt);

			try
			{
				while (rs.Next())
				{
					Record record = rs.Record;
					console.Info("Record: " + record.ToString());
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
