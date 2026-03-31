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

public class ScanParallel(Console console) : SyncExample(console)
{
	private int recordCount = 0;

	/// <summary>
	/// Scan all nodes in parallel and read all records in a set.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		console.Info("Scan parallel: namespace=" + args.ns + " set=" + args.set);
		recordCount = 0;
		var begin = DateTime.Now;
		ScanPolicy policy = new();
		client.ScanAll(policy, args.ns, args.set, ScanCallback);

		DateTime end = DateTime.Now;
		double seconds = end.Subtract(begin).TotalSeconds;
		console.Info("Total records returned: " + recordCount);
		console.Info("Elapsed time: " + seconds + " seconds");
		double performance = Math.Round((double)recordCount / seconds);
		console.Info("Records/second: " + performance);

		if (recordCount == 0)
		{
			throw new Exception("ScanParallel verification failed: no records scanned.");
		}

		console.Info("ScanParallel verified: " + recordCount + " records scanned.");
	}

	public void ScanCallback(Key key, Record record)
	{
		// Callbacks must ensure thread safety when ScanAll() is used with ScanPolicy
		// concurrentNodes set to true (default).  In this case, parallel
		// node threads will be sending data to this callback.
		int count = Interlocked.Increment(ref recordCount);

		if ((count % 10000) == 0)
		{
			console.Info("Records " + count);
		}
	}
}
