/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System;

namespace Aerospike.Demo
{
	public class Transaction : SyncExample
	{
		public Transaction(Console console) : base(console)
		{
		}

		/// <summary>
		/// Multi-record transaction.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			TxnReadWrite(client, args);
		}

		private void TxnReadWrite(IAerospikeClient client, Arguments args)
		{
			Txn txn = new();
			console.Info("Begin txn: " + txn.Id);

			try
			{
				var wp = client.WritePolicyDefault;
				wp.Txn = txn;

				console.Info("Run put");
				Key key1 = new(args.ns, args.set, 1);
				client.Put(wp, key1, new Bin("a", "val1"));

				console.Info("Run another put");
				Key key2 = new(args.ns, args.set, 2);
				client.Put(wp, key2, new Bin("b", "val2"));

				console.Info("Run get");
				var p = client.ReadPolicyDefault;
				p.Txn = txn;

				Key key3 = new(args.ns, args.set, 3);
				Record rec = client.Get(p, key3);

				console.Info("Run delete");
				var dp = client.WritePolicyDefault;
				dp.Txn = txn;
				dp.durableDelete = true;  // Required when running delete in a MRT.
				client.Delete(dp, key3);
			}
			catch (Exception) 
			{
				// Abort and rollback MRT (multi-record transaction) if any errors occur.
				console.Info("Abort txn: " + txn.Id);
				client.Abort(txn);
				throw;
			}

			console.Info("Commit txn: " + txn.Id);
			client.Commit(txn);
		}
	}
}
