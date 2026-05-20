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

public sealed class QuerySum : SyncExample
{
	/// <summary>
	/// Sum a bin across query results using a registered stream UDF.
	/// </summary>
	public override void RunExample()
	{
		const string indexName = "aggindex";
		const string binName = "aggbin";
		const int begin = 4;
		const int end = 7;

		console.Info($"Query for: ns={ns} set={set} index={indexName} bin={binName} >= {begin} <= {end}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			BinNames = [binName],
			Filter = Filter.Range(binName, begin, end)
		};
		stmt.SetAggregateFunction("sum_example", "sum_single_bin", Value.Get(binName));

		using ResultSet rs = client.QueryAggregate(null, stmt);

		while (rs.Next())
		{
			console.Info($"Sum: {rs.Object}");
		}
	}
}
