/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
	public class AsyncScan : AsyncExample
	{
		private int recordCount;
		private bool completed;

		public AsyncScan(Console console) : base(console)
		{
		}

		/// <summary>
		/// Asynchronous scan example.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			console.Info("Asynchronous scan: namespace=" + args.ns + " set=" + args.set);
			recordCount = 0;
			completed = false;

			DateTime begin = DateTime.Now;
			ScanPolicy policy = new ScanPolicy();
			client.ScanAll(policy, new RecordSequenceHandler(this, begin), args.ns, args.set);

			WaitTillComplete();
		}

		private class RecordSequenceHandler : RecordSequenceListener
		{
			private readonly AsyncScan parent;
			private DateTime begin;

			public RecordSequenceHandler(AsyncScan parent, DateTime begin)
			{
				this.parent = parent;
				this.begin = begin;
			}

			public void OnRecord(Key key, Record record)
			{
				int count = Interlocked.Increment(ref parent.recordCount);

				if ((count % 10000) == 0)
				{
					parent.console.Info("Records " + count);
				}
			}

			public void OnSuccess()
			{
				DateTime end = DateTime.Now;
				double seconds = end.Subtract(begin).TotalSeconds;
				parent.console.Info("Total records returned: " + parent.recordCount);
				parent.console.Info("Elapsed time: " + seconds + " seconds");
				double performance = Math.Round((double)parent.recordCount / seconds);
				parent.console.Info("Records/second: " + performance);

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
