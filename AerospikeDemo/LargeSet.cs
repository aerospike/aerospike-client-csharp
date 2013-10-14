using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class LargeSet : SyncExample
	{
		public LargeSet(Console console) : base(console)
		{
		}

		/// <summary>
		/// Perform operations on a list within a single bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Large set functions are not supported by the connected Aerospike server.");
				return;
			}

			Key key = new Key(args.ns, args.set, "setkey");
			string binName = args.GetBinName("setbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large set operator.
			Aerospike.Client.LargeSet set = client.GetLargeSet(args.policy, key, binName, null);

			// Write values.
			set.Add(Value.Get("setvalue1"));
			set.Add(Value.Get("setvalue2"));

			// Verify large set was created with default configuration.
			Dictionary<object,object> map = set.GetConfig();

			foreach (KeyValuePair<object,object> entry in map)
			{
				console.Info(entry.Key.ToString() + ',' + entry.Value);
			}

			int size = set.Size();

			if (size != 2)
			{
				throw new Exception("Size mismatch. Expected 2 Received " + size);
			}

			string received = (string)set.Get(Value.Get("setvalue2"));
			string expected = "setvalue2";

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} value={3}", key.ns, key.setName, key.userKey, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}