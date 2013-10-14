using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class LinearPutGet : SyncExample
	{
		public LinearPutGet(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Write/Read large blocks of data and measure performance.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			int size = 100000;
			string keyPrefix = "linearkey";
			string bin = args.singleBin ? "" : "linearbin";  // Single bin servers don't need a bin name.
			WriteRecords(client, args, keyPrefix, bin, size);
			ReadRecords(client, args, keyPrefix, bin, size);
		}

		private void WriteRecords
		(
			AerospikeClient client, 
			Arguments args, 
			string keyPrefix,
			string bin,
			int size
		)
		{
			if (!valid)
				return;

			// Write multiple records.
			console.Info("Write " + size + " records.");
			WritePolicy policy = new WritePolicy();
			DateTime begin = DateTime.Now;
			int i = 1;

			for (; i <= size && valid; i++)
			{
				string key = keyPrefix + i;
				string value = i.ToString();

				client.Put(policy, new Key(args.ns, args.set, key), new Bin(bin, value));

				if (i % 10000 == 0)
				{
					console.Info("Records " + i);
				}
			}
			DateTime end = DateTime.Now;
			TimeSpan ts = end.Subtract(begin);
			size = i - 1;
			console.Info("Records written: " + size);
			console.Info("Elapsed time: " + ts.ToString());
			double performance = Math.Round(((double)size) / ((double)ts.TotalSeconds), 0);
			console.Info("Writes/second: " + performance);
		}

		private void ReadRecords
		(
			AerospikeClient cs, 
			Arguments args, 
			string keyPrefix,
			string bin, 
			int size
		)
		{
			if (!valid)
				return;

			// Read multiple records.
			console.Info("Read " + size + " records.");
			Policy policy = new Policy();
			DateTime begin = DateTime.Now;
			int i = 1;

			for (; i <= size && valid; i++)
			{
				string key = keyPrefix + i;
				string expected = i.ToString();

				Record record = cs.Get(policy, new Key(args.ns, args.set, key), bin);
				object value = record.GetValue(bin);

				if (! value.Equals(expected))
				{
					console.Error("Set/Get mismatch: Expected {0}. Received {1}.",
						expected, value);
				}

				if (i % 10000 == 0)
				{
					console.Info("Records " + i);
				}
			}
			DateTime end = DateTime.Now;
			TimeSpan ts = end.Subtract(begin);
			size = i - 1;
			console.Info("Records read: " + size);
			console.Info("Elapsed time: " + ts.ToString());
			double performance = Math.Round(((double)size) / ((double)ts.TotalSeconds), 0);
			console.Info("Reads/second: " + performance);
		}
	}
}
