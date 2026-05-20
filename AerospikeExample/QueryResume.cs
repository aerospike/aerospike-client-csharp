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

public sealed class QueryResume : SyncExample
{
	private const string SetName = "qr";
	private const string BinName = "bin";

	private int recordCount;
	private int recordMax;

	/// <summary>
	/// Terminate a query partway through and resume it from the same partition filter.
	/// </summary>
	public override void RunExample()
	{
		Statement stmt = new()
		{
			Namespace = ns,
			SetName = SetName,
			BinNames = [BinName],
			Filter = Filter.Range(BinName, 1, 200)
		};

		PartitionFilter filter = PartitionFilter.All();

		console.Info("Start query");
		recordCount = 0;
		recordMax = 50;

		try
		{
			client.Query(null, stmt, filter, QueryListener);
		}
		catch (AerospikeException.QueryTerminated)
		{
			console.Info("Query terminated as expected");
		}

		console.Info($"Records returned: {recordCount}");

		// PartitionFilter could be serialized at this point. Resume the query now.
		recordCount = 0;
		recordMax = 0;

		console.Info("Start query resume");
		client.Query(null, stmt, filter, QueryListener);
		console.Info($"Records returned: {recordCount}");
	}

	private void QueryListener(Key key, Record record)
	{
		int count = Interlocked.Increment(ref recordCount);

		if (recordMax > 0 && count >= recordMax)
		{
			// Terminating the query rolls back the last-record-key so the current
			// record is returned again when the query is resumed later. This is the
			// safe shape for handling downstream failures (e.g. disk-full on backup).
			throw new AerospikeException.QueryTerminated();
		}
	}
}
