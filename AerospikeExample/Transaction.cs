/* 
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using Aerospike.Client;

namespace Aerospike.Example;

public class Transaction(Console console) : SyncExample(console)
{

	/// <summary>
	/// Transaction.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RequireEnterprise(args);
		RequireStrongConsistency(args);
		TxnReadWrite(client, args);
	}

	private void TxnReadWrite(IAerospikeClient client, Arguments args)
	{
		using Txn txn = new();
		console.Info("Begin txn: " + txn.Id);

		try
		{
			WritePolicy wp = new(client.WritePolicyDefault)
			{
				Txn = txn
			};

			console.Info("Run put");
			Key key1 = new(args.ns, args.set, 1);
			client.Put(wp, key1, new Bin("a", "val1"));

			console.Info("Run another put");
			Key key2 = new(args.ns, args.set, 2);
			client.Put(wp, key2, new Bin("b", "val2"));

			console.Info("Run get");
			Policy p = new(client.ReadPolicyDefault)
			{
				Txn = txn
			};

			Key key3 = new(args.ns, args.set, 3);
			Record rec = client.Get(p, key3);

			console.Info("Run delete");
			WritePolicy dp = new(client.WritePolicyDefault)
			{
				Txn = txn,
				durableDelete = true  // Required when running delete in a transaction.
			};
			client.Delete(dp, key3);
		}
		catch (Exception)
		{
			// Abort and rollback transaction if any errors occur.
			console.Info("Abort txn: " + txn.Id);
			client.Abort(txn);
			throw;
		}

		console.Info("Commit txn: " + txn.Id);
		client.Commit(txn);

		// Verify the committed data persisted.
		Key verifyKey1 = new(args.ns, args.set, 1);
		Record r1 = client.Get(null, verifyKey1);
		if (r1 == null || !"val1".Equals(r1.GetValue("a")))
		{
			throw new Exception("Transaction verify failed: key 1 bin 'a' expected 'val1'");
		}

		Key verifyKey2 = new(args.ns, args.set, 2);
		Record r2 = client.Get(null, verifyKey2);
		if (r2 == null || !"val2".Equals(r2.GetValue("b")))
		{
			throw new Exception("Transaction verify failed: key 2 bin 'b' expected 'val2'");
		}

		console.Info("Transaction verified: writes persisted and commit confirmed.");
	}
}
