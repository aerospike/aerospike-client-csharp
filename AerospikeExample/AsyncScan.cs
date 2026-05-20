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

public sealed class AsyncScan : AsyncExample
{
	private readonly ManualResetEventSlim completed = new();
	private int recordCount;

	/// <summary>
	/// Scan all records asynchronously across all nodes, streaming each record to a listener.
	/// </summary>
	public override void RunExample()
	{
		console.Info($"Asynchronous scan: namespace={ns} set={set}");

		recordCount = 0;
		completed.Reset();

		ScanPolicy scanPolicy = new();
		client.ScanAll(scanPolicy, new RecordSequenceHandler(this), ns, set);

		completed.Wait();
	}

	private void NotifyComplete() => completed.Set();

	private sealed class RecordSequenceHandler(AsyncScan parent) : RecordSequenceListener
	{
		private readonly DateTime begin = DateTime.Now;

		public void OnRecord(Key key, Record record)
		{
			int count = Interlocked.Increment(ref parent.recordCount);

			if (count % 10000 == 0)
			{
				parent.console.Info($"Records {count}");
			}
		}

		public void OnSuccess()
		{
			double seconds = (DateTime.Now - begin).TotalSeconds;
			parent.console.Info($"Total records returned: {parent.recordCount}");
			parent.console.Info($"Elapsed time: {seconds} seconds");
			parent.console.Info($"Records/second: {Math.Round(parent.recordCount / seconds)}");
			parent.NotifyComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Scan failed: {Util.GetErrorMessage(e)}");
			parent.NotifyComplete();
		}
	}
}
