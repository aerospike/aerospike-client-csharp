using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Replace : SyncExample
	{
		public Replace(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Demonstrate writing bins with replace option. Replace will cause all record bins
		/// to be overwritten.  If an existing bin is not referenced in the replace command,
		/// the bin will be deleted.
		/// <para>
		/// The replace command has a performance advantage over the default put, because 
		/// the server does not have to read the existing record before overwriting it.
		/// </para>
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			RunReplaceExample(client, args);
			RunReplaceOnlyExample(client, args);
		}

		private void RunReplaceExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "replacekey");
			Bin bin1 = new Bin("bin1", "value1");
			Bin bin2 = new Bin("bin2", "value2");
			Bin bin3 = new Bin("bin3", "value3");

			console.Info("Put: namespace={0} set={1} key={2} bin1={3} value1={4} bin2={5} value2={6}", 
				key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

			client.Put(args.writePolicy, key, bin1, bin2);

			console.Info("Replace with: namespace={0} set={1} key={2} bin={3} value={4}", 
				key.ns, key.setName, key.userKey, bin3.name, bin3.value);

			WritePolicy policy = new WritePolicy();
			policy.recordExistsAction = RecordExistsAction.REPLACE;
			client.Put(policy, key, bin3);

			console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

			Record record = client.Get(args.policy, key);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			if (record.GetValue(bin1.name) == null)
			{
				console.Info(bin1.name + " was deleted as expected.");
			}
			else
			{
				console.Error(bin1.name + " found when it should have been deleted.");
			}

			if (record.GetValue(bin2.name) == null)
			{
				console.Info(bin2.name + " was deleted as expected.");
			}
			else
			{
				console.Error(bin2.name + " found when it should have been deleted.");
			}
			ValidateBin(key, bin3, record);
		}

		private void RunReplaceOnlyExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "replaceonlykey");
			Bin bin = new Bin("bin", "value");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			console.Info("Replace record requiring that it exists: namespace={0} set={1} key={2}",
				key.ns, key.setName, key.userKey);

			try
			{
				WritePolicy policy = new WritePolicy();
				policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
				client.Put(policy, key, bin);

				console.Error("Failure. This command should have resulted in an error.");
			}
			catch (AerospikeException ae)
			{
				if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					console.Info("Success. Key not found error returned as expected.");
				}
				else
				{
					throw ae;
				}
			}
		}

		private void ValidateBin(Key key, Bin bin, Record record)
		{
			object received = record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}",
					key.ns, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}", expected, received);
			}
		}
	}
}