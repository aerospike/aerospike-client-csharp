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

public sealed class Transaction : SyncExample
{
	/// <summary>
	/// Run a read/write/delete transaction (requires Enterprise edition with strong consistency).
	/// </summary>
	public override void RunExample()
	{
		RequireEnterprise();
		RequireStrongConsistency();
		TxnReadWrite();
	}

	private void TxnReadWrite()
	{
		using Txn txn = new();
		Console.WriteLine($"Begin txn: {txn.Id}");

		try
		{
			WritePolicy wp = new(client.WritePolicyDefault)
			{
				Txn = txn
			};

			Console.WriteLine("Run put");
			Key key1 = new(ns, set, 1);
			client.Put(wp, key1, new Bin("a", "val1"));

			Console.WriteLine("Run another put");
			Key key2 = new(ns, set, 2);
			client.Put(wp, key2, new Bin("b", "val2"));

			Console.WriteLine("Run get");
			Policy p = new(client.ReadPolicyDefault)
			{
				Txn = txn
			};

			Key key3 = new(ns, set, 3);
			Record rec = client.Get(p, key3);
			Console.WriteLine($"Get result: {rec}");

			Console.WriteLine("Run delete");
			WritePolicy dp = new(client.WritePolicyDefault)
			{
				Txn = txn,
				durableDelete = true // Required when running delete inside a transaction.
			};
			client.Delete(dp, key3);
		}
		catch
		{
			Console.WriteLine($"Abort txn: {txn.Id}");
			client.Abort(txn);
			throw;
		}

		Console.WriteLine($"Commit txn: {txn.Id}");
		client.Commit(txn);
	}
}
