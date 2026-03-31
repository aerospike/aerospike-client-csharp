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

public class AsyncScanPage(Console console) : AsyncExample(console)
{
	private const string BinName = "bin";
	private const string SetName = "apage";
	private const int Size = 200;

	private AsyncClient client;
	private Arguments args;
	private bool completed;

	/// <summary>
	/// Asynchronous scan page example.
	/// </summary>
	public override void RunExample(AsyncClient client, Arguments args)
	{
		this.client = client;
		this.args = args;
		this.completed = false;

		// Scan will be called after WriteRecords completes.
		WriteRecords();
		WaitTillComplete();

		int recordCount = 0;
		ScanPolicy verifyScanPolicy = new();
		client.ScanAll(verifyScanPolicy, args.ns, SetName, (Key k, Record r) =>
		{
			Interlocked.Increment(ref recordCount);
		});
		if (recordCount == 0)
		{
			throw new Exception("AsyncScanPage verification failed: no records scanned.");
		}
		console.Info("AsyncScanPage verified: " + recordCount + " records.");
	}

	private void WriteRecords()
	{
		console.Info("Write " + Size + " records.");

		var handler = new WriteHandler(this, Size);

		for (int i = 1; i <= Size; i++)
		{
			var key = new Key(args.ns, SetName, i);
			var bin = new Bin(BinName, i);
			client.Put(args.writePolicy, handler, key, bin);
		}
	}

	private class WriteHandler(AsyncScanPage parent, int max) : WriteListener
	{
		private readonly AsyncScanPage parent = parent;
		private readonly int max = max;
		private int count;

		public void OnSuccess(Key key)
		{
			int rows = Interlocked.Increment(ref count);

			if (rows == max)
			{
				try
				{
					// All writes succeeded. Run scan page.
					parent.ScanPage();
				}
				catch (Exception e)
				{
					parent.console.Error("Scan failed: " + e.Message);
					parent.NotifyComplete();
				}
			}
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Put failed: " + e.Message);
			parent.NotifyComplete();
		}
	}

	private void ScanPage()
	{
		console.Info("Scan page async");

		ScanPolicy policy = new()
		{
			maxRecords = 100
		};

		var filter = PartitionFilter.All();

		client.ScanPartitions(policy, new RecordSequenceHandler(this), filter, args.ns, SetName);
	}

	private class RecordSequenceHandler(AsyncScanPage parent) : RecordSequenceListener
	{
		private readonly AsyncScanPage parent = parent;
		private int recordCount;

		public void OnRecord(Key key, Record record)
		{
			Interlocked.Increment(ref recordCount);
		}

		public void OnSuccess()
		{
			parent.console.Info("Records returned: " + recordCount);
			parent.NotifyComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Scan failed: " + Util.GetErrorMessage(e));
			parent.NotifyComplete();
		}
	}

	private void WaitTillComplete()
	{
		lock (this)
		{
			while (!completed)
			{
				Monitor.Wait(this);
			}
		}
	}

	private void NotifyComplete()
	{
		lock (this)
		{
			completed = true;
			Monitor.Pulse(this);
		}
	}
}
