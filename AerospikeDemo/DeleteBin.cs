using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class DeleteBin : SyncExample
	{
		public DeleteBin(Console console) : base(console)
		{
		}

		/// <summary>
		/// Drop a bin from a record.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (args.singleBin)
			{
				console.Info("Delete bin is not applicable to single bin servers.");
				return;
			}

			console.Info("Write multi-bin record.");
			Key key = new Key(args.ns, args.set, "delbinkey");
			string binName1 = args.GetBinName("bin1");
			string binName2 = args.GetBinName("bin2");
			Bin bin1 = new Bin(binName1, "value1");
			Bin bin2 = new Bin(binName2, "value2");
			client.Put(args.writePolicy, key, bin1, bin2);

			console.Info("Delete one bin in the record.");
			bin1 = Bin.AsNull(binName1); // Set bin value to null to drop bin.
			client.Put(args.writePolicy, key, bin1);

			console.Info("Read record.");
			Record record = client.Get(args.policy, key, bin1.name, bin2.name, "bin3");

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			foreach (KeyValuePair<string, object> entry in record.bins)
			{
				console.Info("Received: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, entry.Key, entry.Value);
			}

			bool valid = true;

			if (record.GetValue("bin1") != null)
			{
				console.Error("bin1 still exists.");
				valid = false;
			}

			object v2 = record.GetValue("bin2");

			if (v2 == null || !v2.Equals("value2"))
			{
				console.Error("bin2 value mismatch.");
				valid = false;
			}

			if (valid)
			{
				console.Info("Bin delete successful");
			}
		}
	}
}