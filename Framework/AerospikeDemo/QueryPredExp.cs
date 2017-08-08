/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System;
using System.IO;
using Aerospike.Client;
using System.Collections.Generic;

namespace Aerospike.Demo
{
	public class QueryPredExp : SyncExample
	{
		public QueryPredExp(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Perform secondary index queries with predicate filters.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}
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

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string binName, int size)
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

		private void RunQuery1(AerospikeClient client, Arguments args, string binName)
		{
			int begin = 10;
			int end = 40;

			console.Info("Query Predicate: (bin2 > 126 && bin2 <= 140) or (bin2 = 360)");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);

			// Filter applied on query itself.  Filter can only reference an indexed bin.
			stmt.SetFilter(Filter.Range(binName, begin, end));

			// Predicates are applied on query results on server side.
			// Predicates can reference any bin.
			stmt.SetPredExp(
				PredExp.IntegerBin("bin2"), 
				PredExp.IntegerValue(126),
				PredExp.IntegerGreater(),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(140),
				PredExp.IntegerLessEq(),
				PredExp.And(2),
				PredExp.IntegerBin("bin2"),
				PredExp.IntegerValue(360),
				PredExp.IntegerEqual(),
				PredExp.Or(2)
				);

			RecordSet rs = client.Query(null, stmt);

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

		private void RunQuery2(AerospikeClient client, Arguments args, string binName)
		{
			int begin = 10;
			int end = 40;

			console.Info("Query Predicate: Record updated on 2017-01-15");
			DateTime beginTime = new DateTime(2017, 1, 15);
			DateTime endTime = new DateTime(2017, 1, 16);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.RecLastUpdate(),
				PredExp.IntegerValue(beginTime),
				PredExp.IntegerGreaterEq(),
				PredExp.RecLastUpdate(),
				PredExp.IntegerValue(endTime),
				PredExp.IntegerLess(),
				PredExp.And(2)
				);

			RecordSet rs = client.Query(null, stmt);

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

		private void RunQuery3(AerospikeClient client, Arguments args, string binName)
		{
			int begin = 20;
			int end = 30;

			console.Info("Query Predicate: bin3 contains string with 'prefix' and 'suffix'");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName, begin, end));
			stmt.SetPredExp(
				PredExp.StringBin("bin3"),
				PredExp.StringValue("prefix.*suffix"),
				PredExp.StringRegex(RegexFlag.ICASE | RegexFlag.NEWLINE)
				);

			RecordSet rs = client.Query(null, stmt);

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
