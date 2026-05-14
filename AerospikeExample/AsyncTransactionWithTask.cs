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

public class AsyncTransactionWithTask(Console console) : AsyncExample(console)
{

	/// <summary>
	/// Transaction.
	/// </summary>
	public override void RunExample(AsyncClient client, Arguments args)
	{
		RequireEnterprise(args);
		RequireStrongConsistency(args);

		using Txn txn = new();
		var token = CancellationToken.None;

		console.Info("Begin txn: " + txn.Id);

		try
		{
			Key key = null;

			var result = Task.Run(async () =>
			{
				try
				{
					WritePolicy wp = new(client.WritePolicyDefault)
					{
						Txn = txn
					};

					console.Info("Run put with task");
					key = new(args.ns, args.set, 1);
					await client.Put(wp, token, key, new Bin("a", "val1"));

					console.Info("Run another put");
					key = new(args.ns, args.set, 2);
					await client.Put(wp, token, key, new Bin("b", "val2"));

					console.Info("Run get");
					Policy p = new(client.ReadPolicyDefault)
					{
						Txn = txn
					};
					Key key3 = new(args.ns, args.set, 3);
					Record rec = await client.Get(p, token, key3);

					console.Info("Run delete");
					WritePolicy dp = new(client.WritePolicyDefault)
					{
						Txn = txn,
						durableDelete = true  // Required when running delete in a transaction.
					};
					client.Delete(dp, key3);

					await client.Commit(txn, token);
					return true;
				}
				catch (Exception e)
				{
					console.Error("Failed to write: namespace={0} set={1} key={2} exception={3}",
									key.ns, key.setName, key.userKey, e.Message);
					// Abort and rollback transaction if any errors occur.
					await client.Abort(txn, token);
					return false;
				}
			}).Result;

			if (result)
			{
				console.Info("Txn committed: " + txn.Id);

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
			else
			{
				console.Error("Txn aborted: " + txn.Id);
			}
		}
		catch (Exception e)
		{
			console.Error($"Txn {txn.Id} Exception: {e}");
		}
	}
}
