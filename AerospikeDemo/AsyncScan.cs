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