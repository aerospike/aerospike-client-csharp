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

public sealed class AsyncScanPage : AsyncExample
{
	private const string SetName = "apage";

	private readonly ManualResetEventSlim completed = new();

	/// <summary>
	/// Scan a single page of records asynchronously, capped by maxRecords.
	/// </summary>
	public override void RunExample()
	{
		completed.Reset();

		console.Info("Scan page async");

		ScanPolicy scanPolicy = new()
		{
			maxRecords = 100
		};

		PartitionFilter filter = PartitionFilter.All();
		client.ScanPartitions(scanPolicy, new RecordSequenceHandler(this), filter, ns, SetName);
		completed.Wait();
	}

	private void NotifyComplete() => completed.Set();

	private sealed class RecordSequenceHandler(AsyncScanPage parent) : RecordSequenceListener
	{
		private int recordCount;

		public void OnRecord(Key key, Record record) => Interlocked.Increment(ref recordCount);

		public void OnSuccess()
		{
			parent.console.Info($"Records returned: {recordCount}");
			parent.NotifyComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Scan failed: {Util.GetErrorMessage(e)}");
			parent.NotifyComplete();
		}
	}
}
