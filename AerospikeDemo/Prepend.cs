using System;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Prepend : SyncExample
	{
		public Prepend(Console console) : base(console)
		{
		}

		/// <summary>
		/// Prepend string to an existing string.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "prependkey");
			string binName = args.GetBinName("prependbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			Bin bin = new Bin(binName, "World");
			console.Info("Initial prepend will create record.  Initial value is " + bin.value + '.');
			client.Prepend(args.writePolicy, key, bin);

			bin = new Bin(binName, "Hello ");
			console.Info("Prepend \"" + bin.value + "\" to existing record.");
			client.Prepend(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			// The value received from the server is an unsigned byte stream.
			// Convert to an integer before comparing with expected.
			object received = record.GetValue(bin.name);
			string expected = "Hello World";

			if (received.Equals(expected))
			{
				console.Info("Prepend successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Prepend mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}