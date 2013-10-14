using System;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Generation : SyncExample
	{
		public Generation(Console console) : base(console)
		{
		}

		/// <summary>
		/// Exercise record generation functionality.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "genkey");
			string binName = args.GetBinName("genbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Set some values for the same record.
			Bin bin = new Bin(binName, "genvalue1");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			client.Put(args.writePolicy, key, bin);

			bin = new Bin(binName, "genvalue2");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			client.Put(args.writePolicy, key, bin);

			// Retrieve record and its generation count.
			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			object received = record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received.Equals(expected))
			{
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4} generation={5}", 
					key.ns, key.setName, key.userKey, bin.name, received, record.generation);
			}
			else
			{
				throw new Exception(string.Format("Get mismatch: Expected {0}. Received {1}.", expected, received));
			}

			// Set record and fail if it's not the expected generation.
			bin = new Bin(binName, "genvalue3");
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expected generation={5}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value, record.generation);

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.recordExistsAction = RecordExistsAction.EXPECT_GEN_EQUAL;
			writePolicy.generation = record.generation;
			client.Put(writePolicy, key, bin);

			// Set record with invalid generation and check results .
			bin = new Bin(binName, "genvalue4");
			writePolicy.generation = 9999;
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expected generation={5}", 
				key.ns, key.setName, key.userKey, bin.name, bin.value, writePolicy.generation);

			try
			{
				client.Put(writePolicy, key, bin);
				throw new Exception("Should have received generation error instead of success.");
			}
			catch (AerospikeException ae)
			{
				if (ae.Result == ResultCode.GENERATION_ERROR)
				{
					console.Info("Success: Generation error returned as expected.");
				}
				else
				{
					throw new Exception(string.Format("Unexpected set return code: namespace={0} set={1} key={2} bin={3} value={4} code={5}", 
						key.ns, key.setName, key.userKey, bin.name, bin.value, ae.Result));
				}
			}

			// Verify results.
			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			received = record.GetValue(bin.name);
			expected = "genvalue3";

			if (received.Equals(expected))
			{
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4} generation={5}", 
					key.ns, key.setName, key.userKey, bin.name, received, record.generation);
			}
			else
			{
				throw new Exception(string.Format("Get mismatch: Expected {0}. Received {1}.", expected, received));
			}
		}
	}
}