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

public sealed class QueryExp : SyncExample
{
	private const string BinName = "idxbin";

	/// <summary>
	/// Run secondary-index queries layered with server-side expression filters.
	/// </summary>
	public override void RunExample()
	{
		RunNumericPredicate();
		RunTimePredicate();
		RunRegexPredicate();
	}

	private void RunNumericPredicate()
	{
		console.Info("Query Predicate: (bin2 > 126 && bin2 <= 140) || (bin2 = 360)");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Range(BinName, 10, 40)
		};

		QueryPolicy queryPolicy = new(client.QueryPolicyDefault)
		{
			filterExp = Exp.Build(
				Exp.Or(
					Exp.And(
						Exp.GT(Exp.IntBin("bin2"), Exp.Val(126)),
						Exp.LE(Exp.IntBin("bin2"), Exp.Val(140))),
					Exp.EQ(Exp.IntBin("bin2"), Exp.Val(360))))
		};

		PrintRecords(queryPolicy, stmt);
	}

	private void RunTimePredicate()
	{
		console.Info("Query Predicate: Record updated in 2020");

		DateTime beginTime = new(2020, 1, 1);
		DateTime endTime = new(2021, 1, 1);

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Range(BinName, 10, 40)
		};

		QueryPolicy queryPolicy = new(client.QueryPolicyDefault)
		{
			filterExp = Exp.Build(
				Exp.And(
					Exp.GE(Exp.LastUpdate(), Exp.Val(beginTime)),
					Exp.LT(Exp.LastUpdate(), Exp.Val(endTime))))
		};

		PrintRecords(queryPolicy, stmt);
	}

	private void RunRegexPredicate()
	{
		console.Info("Query Predicate: bin3 contains string with 'prefix' and 'suffix'");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Range(BinName, 20, 30)
		};

		QueryPolicy queryPolicy = new(client.QueryPolicyDefault)
		{
			filterExp = Exp.Build(
				Exp.RegexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.StringBin("bin3")))
		};

		PrintRecords(queryPolicy, stmt);
	}

	private void PrintRecords(QueryPolicy queryPolicy, Statement stmt)
	{
		using RecordSet rs = client.Query(queryPolicy, stmt);

		while (rs.Next())
		{
			console.Info($"Record: {rs.Record}");
		}
	}
}
