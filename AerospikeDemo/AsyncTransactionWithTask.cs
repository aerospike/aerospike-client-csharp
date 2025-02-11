/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using Neo.IronLua;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.TextBox;

namespace Aerospike.Demo
{
	public class AsyncTransactionWithTask : AsyncExample
	{
		private bool completed;

		public AsyncTransactionWithTask(Console console) : base(console)
		{
		}

		/// <summary>
		/// Transaction.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			Txn txn = new();
			var token = CancellationToken.None;

			console.Info("Begin txn: " + txn.Id);

			try
			{
				Key key = null;

				var result = Task.Run(async () =>
				{
					try
					{
						WritePolicy wp = client.WritePolicyDefault;
						wp.Txn = txn;

						console.Info("Run put with task");
						key = new(args.ns, args.set, 1);
						await client.Put(wp, token, key, new Bin("a", "val1"));

						console.Info("Run another put");
						key = new(args.ns, args.set, 2);
						await client.Put(wp, token, key, new Bin("b", "val2"));

						console.Info("Run get");
						var p = client.ReadPolicyDefault;
						p.Txn = txn;
						Key key3 = new(args.ns, args.set, 3);
						Record rec = await client.Get(p, token, key3);

						console.Info("Run delete");
						var dp = client.WritePolicyDefault;
						dp.Txn = txn;
						dp.durableDelete = true;  // Required when running delete in a transaction.
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
					console.Info("Txn committed: " + txn.Id);
				else
					console.Error("Txn aborted: " + txn.Id);
			}
			catch (Exception e)
			{
				console.Error($"Txn {txn.Id} Exception: {e}");
			}
		}
	}
}
