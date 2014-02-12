/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
				parent.recordCount++;

				if ((parent.recordCount % 10000) == 0)
				{
					parent.console.Info("Records " + parent.recordCount);
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
