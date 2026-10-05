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

/// <summary>
/// Demonstrate Top-K queries using <see cref="Statement.SetOrderBy"/> + <see cref="Statement.SetTopK"/>.
/// Supporting server nodes return bounded candidates; the client merges them into global Top-K
/// results and falls back to client-side reduction for mixed clusters.
/// </summary>
public sealed class QueryTopK : SyncExample
{
	public override void RunExample()
	{
		const string indexName = "topkindex";
		const string keyPrefix = "topkkey";
		const string binName = "topkbin";
		const int size = 20;
		const int k = 5;

		CreateIndex(indexName, binName);
		WriteRecords(keyPrefix, binName, size);
		RunQuery(binName, k);
		client.DropIndex(null, ns, set, indexName);
	}

	private void CreateIndex(string indexName, string binName)
	{
		Console.WriteLine($"Create index: ns={ns} set={set} index={indexName} bin={binName}");

		Policy policy = new() { totalTimeout = 0 };

		try
		{
			IndexTask task = client.CreateIndex(policy, ns, set, indexName, binName, IndexType.INTEGER);
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

	private void WriteRecords(string keyPrefix, string binName, int size)
	{
		Console.WriteLine($"Write {size} records.");

		for (int i = 1; i <= size; i++)
		{
			Key key = new(ns, set, keyPrefix + i);
			client.Put(writePolicy, key, new Bin(binName, i));
		}
	}

	private void RunQuery(string binName, int k)
	{
		const int begin = 1;
		const int end = 1000;

		Console.WriteLine($"Query top {k} by bin={binName} descending");

		Statement stmt = new();
		stmt.SetNamespace(ns);
		stmt.SetSetName(set);
		stmt.SetBinNames(binName);
		stmt.SetFilter(Filter.Range(binName, begin, end));
		stmt.SetOrderBy(binName, BinDataType.INTEGER, Order.DESC);
		stmt.SetTopK(k);

		using RecordSet rs = client.Query(null, stmt);
		int count = 0;
		int expected = 20;

		while (rs.Next())
		{
			int value = rs.Record.GetInt(binName);
			Console.WriteLine($"Top-K record: {binName}={value}");

			if (value != expected)
			{
				Console.WriteLine($"Top-K order mismatch. Expected {expected}. Received {value}.");
			}
			expected--;
			count++;
		}

		if (count != k)
		{
			Console.WriteLine($"Top-K count mismatch. Expected {k}. Received {count}.");
		}
	}
}
