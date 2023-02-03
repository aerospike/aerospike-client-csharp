/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class AsyncScanPage : AsyncExample
	{
		private const string BinName = "bin";
		private const string SetName = "apage";
		private const int Size = 200;

		private AsyncClient client;
		private Arguments args;
		private bool completed;

		public AsyncScanPage(Console console) : base(console)
		{
		}

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
		}

		private void WriteRecords()
		{
			console.Info("Write " + Size + " records.");

			WriteHandler handler = new WriteHandler(this, Size);

			for (int i = 1; i <= Size; i++)
			{
				Key key = new Key(args.ns, SetName, i);
				Bin bin = new Bin(BinName, i);
				client.Put(args.writePolicy, handler, key, bin);
			}
		}

		private class WriteHandler : WriteListener
		{
			private readonly AsyncScanPage parent;
			private readonly int max;
			private int count;

			public WriteHandler(AsyncScanPage parent, int max)
			{
				this.parent = parent;
				this.max = max;
			}

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

			ScanPolicy policy = new ScanPolicy();
			policy.maxRecords = 100;

			PartitionFilter filter = PartitionFilter.All();

			client.ScanPartitions(policy, new RecordSequenceHandler(this), filter, args.ns, SetName);
		}

		private class RecordSequenceHandler : RecordSequenceListener
		{
			private readonly AsyncScanPage parent;
			private int recordCount;

			public RecordSequenceHandler(AsyncScanPage parent)
			{
				this.parent = parent;
			}

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
}
