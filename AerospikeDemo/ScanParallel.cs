using System;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ScanParallel : SyncExample
	{
		private int recordCount = 0;

		public ScanParallel(Console console) : base(console)
		{
		}

		/// <summary>
		/// Scan all nodes in parallel and read all records in a set.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			console.Info("Scan parallel: namespace=" + args.ns + " set=" + args.set);
			recordCount = 0;
			DateTime begin = DateTime.Now;
			ScanPolicy policy = new ScanPolicy();
			client.ScanAll(policy, args.ns, args.set, ScanCallback);

			DateTime end = DateTime.Now;
			double seconds = end.Subtract(begin).TotalSeconds;
			console.Info("Total records returned: " + recordCount);
			console.Info("Elapsed time: " + seconds + " seconds");
			double performance = Math.Round((double)recordCount / seconds);
			console.Info("Records/second: " + performance);
		}

		public void ScanCallback(Key key, Record record)
		{
			recordCount++;

			if ((recordCount % 10000) == 0)
			{
				console.Info("Records " + recordCount);
			}
		}
	}
}