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

public sealed class ScanPage : SyncExample
{
	private const string SetName = "page";

	private int recordCount;

	/// <summary>
	/// Scan a set in fixed-size pages and resume across calls using a partition filter.
	/// </summary>
	public override void RunExample()
	{
		ScanPolicy scanPolicy = new()
		{
			maxRecords = 100
		};

		PartitionFilter filter = PartitionFilter.All();

		for (int i = 0; i < 3 && !filter.Done; i++)
		{
			recordCount = 0;

			console.Info($"Scan page: {i}");
			client.ScanPartitions(scanPolicy, filter, ns, SetName, ScanCallback);
			console.Info($"Records returned: {recordCount}");
		}
	}

	private void ScanCallback(Key key, Record record)
	{
		// Callbacks must be thread-safe when concurrentNodes is true (the default).
		Interlocked.Increment(ref recordCount);
	}
}
