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

public sealed class QueryPage : SyncExample
{
	private const string SetName = "pq";
	private const string BinName = "bin";

	/// <summary>
	/// Iterate a query in fixed-size pages using a partition filter as the resume token.
	/// </summary>
	public override void RunExample()
	{
		Statement stmt = new()
		{
			Namespace = ns,
			SetName = SetName,
			BinNames = [BinName],
			Filter = Filter.Range(BinName, 1, 200),
			MaxRecords = 100
		};

		PartitionFilter filter = PartitionFilter.All();

		for (int i = 0; i < 3 && !filter.Done; i++)
		{
			console.Info($"Query page: {i}");

			using RecordSet rs = client.QueryPartitions(null, stmt, filter);

			int count = 0;

			while (rs.Next())
			{
				count++;
			}

			console.Info($"Records returned: {count}");
		}
	}
}
