using System;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Append : SyncExample
	{
		public Append(Console console) : base(console)
		{
		}

		/// <summary>
		/// Append string to an existing string.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "appendkey");
			string binName = args.GetBinName("appendbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			Bin bin = new Bin(binName, "Hello");
			console.Info("Initial append will create record.  Initial value is " + bin.value + '.');
			client.Append(args.writePolicy, key, bin);

			bin = new Bin(binName, " World");
			console.Info("Append \"" + bin.value + "\" to existing record.");
			client.Append(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			object received = record.GetValue(bin.name);
			string expected = "Hello World";

			if (received.Equals(expected))
			{
				console.Info("Append successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Append mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}