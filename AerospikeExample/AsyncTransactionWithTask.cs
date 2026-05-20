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

public sealed class AsyncTransactionWithTask : AsyncExample
{
	/// <summary>
	/// Run a read/write/delete transaction asynchronously using Task-based async/await.
	/// </summary>
	public override void RunExample()
	{
		RequireEnterprise();
		RequireStrongConsistency();

		RunTransaction().GetAwaiter().GetResult();
	}

	private async Task RunTransaction()
	{
		using Txn txn = new();
		CancellationToken token = CancellationToken.None;
		console.Info($"Begin txn: {txn.Id}");

		WritePolicy wp = new(client.WritePolicyDefault)
		{
			Txn = txn
		};

		try
		{
			console.Info("Run put with task");
			Key key1 = new(ns, set, 1);
			await client.Put(wp, token, key1, new Bin("a", "val1"));

			console.Info("Run another put");
			Key key2 = new(ns, set, 2);
			await client.Put(wp, token, key2, new Bin("b", "val2"));

			console.Info("Run get");
			Policy p = new(policy)
			{
				Txn = txn
			};
			Key key3 = new(ns, set, 3);
			Record rec = await client.Get(p, token, key3);

			console.Info("Run delete");
			WritePolicy dp = new(writePolicy)
			{
				Txn = txn,
				durableDelete = true // Required when running delete inside a transaction.
			};
			await client.Delete(dp, token, key3);

			await client.Commit(txn, token);
			console.Info($"Txn committed: {txn.Id}");
		}
		catch (Exception e)
		{
			console.Error($"Txn {txn.Id} failed: {e.Message}");
			await client.Abort(txn, token);
			console.Error($"Txn aborted: {txn.Id}");
		}
	}
}
