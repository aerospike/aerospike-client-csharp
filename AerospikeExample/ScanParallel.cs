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

public sealed class ScanParallel : SyncExample
{
	private int recordCount;

	/// <summary>
	/// Scan all nodes in parallel and read every record in a set.
	/// </summary>
	public override void RunExample()
	{
		console.Info($"Scan parallel: namespace={ns} set={set}");
		recordCount = 0;

		DateTime begin = DateTime.Now;
		ScanPolicy scanPolicy = new();
		client.ScanAll(scanPolicy, ns, set, ScanCallback);

		double seconds = (DateTime.Now - begin).TotalSeconds;
		console.Info($"Total records returned: {recordCount}");
		console.Info($"Elapsed time: {seconds} seconds");
		console.Info($"Records/second: {Math.Round(recordCount / seconds)}");
	}

	private void ScanCallback(Key key, Record record)
	{
		// Callbacks must be thread-safe when concurrentNodes is true (the default),
		// because parallel node threads can invoke this concurrently.
		int count = Interlocked.Increment(ref recordCount);

		if (count % 10000 == 0)
		{
			console.Info($"Records {count}");
		}
	}
}
