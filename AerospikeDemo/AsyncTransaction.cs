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
using System.Threading;

namespace Aerospike.Demo
{
	public class AsyncTransaction : AsyncExample
	{
		private bool completed;

		public AsyncTransaction(Console console) : base(console)
		{
		}

		/// <summary>
		/// Transaction.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			completed = false;

			using Txn txn = new();

			console.Info("Begin txn: " + txn.Id);
			Put(client, txn, args);

			WaitTillComplete();
		}

		public void Put(AsyncClient client, Txn txn, Arguments args)
		{
			console.Info("Run put");

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;

			Key key = new(args.ns, args.set, 1);
		
			client.Put(wp, new PutHandler(this, client, key, txn, args), key, new Bin("a", "val1"));
		}

		class PutHandler : WriteListener
		{
			private readonly AsyncTransaction parent;
			private readonly AsyncClient client;
			private readonly Key key;
			private readonly Txn txn;
			private readonly Arguments args;

			public PutHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args) 
			{ 
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.txn = txn;
				this.args = args;
			}
			
			public void OnSuccess(Key key)
			{
				parent.PutAnother(client, txn, args);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to write: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
				parent.Abort(client, txn);
			}
		};

		public void PutAnother(AsyncClient client, Txn txn, Arguments args)
		{
			console.Info("Run another put");

			var wp = client.WritePolicyDefault;
			wp.Txn = txn;

			Key key = new(args.ns, args.set, 2);

			client.Put(wp, new PutAnotherHandler(this, client, key, txn, args), key, new Bin("b", "val2"));		
		}

		class PutAnotherHandler : WriteListener
		{
			private readonly AsyncTransaction parent;
			private readonly AsyncClient client;
			private readonly Key key;
			private	readonly Txn txn;
			private readonly Arguments args;

			public PutAnotherHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.txn = txn;
				this.args = args;
			}

			public void OnSuccess(Key key)
			{
				parent.Get(client, txn, args);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to write: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
				parent.Abort(client, txn);
			}
		}
	
		public void Get(AsyncClient client, Txn txn, Arguments args)
		{
			console.Info("Run get");

			var p = client.ReadPolicyDefault;
			p.Txn = txn;

			Key key = new(args.ns, args.set, 3);

			client.Get(p, new GetHandler(this, client, key, txn, args), key);
		}

		class GetHandler : RecordListener
		{
			private readonly AsyncTransaction parent;
			private readonly AsyncClient client;
			private readonly Key key;
			private readonly Txn txn;
			private readonly Arguments args;

			public GetHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.txn = txn;
				this.args = args;
			}

			public void OnSuccess(Key key, Record record)
			{
				parent.Delete(client, txn, args);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to read: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
				parent.Abort(client, txn);
			}
		}
	
		public void Delete(AsyncClient client, Txn txn, Arguments args)
		{
			console.Info("Run delete");

			var dp = client.WritePolicyDefault;
			dp.Txn = txn;
			dp.durableDelete = true;  // Required when running delete in a transaction.

			Key key = new(args.ns, args.set, 3);

			client.Delete(dp, new DeleteHandler(this, client, key, txn), key);
		}
		
		class DeleteHandler : DeleteListener
		{
			private readonly AsyncTransaction parent;
			private readonly AsyncClient client;
			private readonly Key key;
			private readonly Txn txn;

			public DeleteHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.txn = txn;
			}

			public void OnSuccess(Key key, bool existed)
			{
				parent.Commit(client, txn);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to delete: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
				parent.Abort(client, txn);
			}

		}
	
		public void Commit(AsyncClient client, Txn txn)
		{
			console.Info("Run commit");

			client.Commit(new CommitHandler(this, txn), txn);
		}

		class CommitHandler : CommitListener
		{
			private readonly AsyncTransaction parent;
			private readonly Txn txn;

			public CommitHandler(AsyncTransaction parent, Txn txn)
			{
				this.parent = parent;
				this.txn = txn;
			}

			public void OnSuccess(CommitStatus.CommitStatusType status)
			{
				parent.console.Info("Txn committed: " + txn.Id);
				parent.NotifyComplete();
			}

			public void OnFailure(AerospikeException.Commit ae)
			{
				parent.console.Error("Txn commit failed: " + txn.Id);
				parent.NotifyComplete();
			}
		}

		public void Abort(AsyncClient client, Txn txn)
		{
			console.Info("Run abort");

			client.Abort(new AbortHandler(this, txn), txn);		
		}
		
		class AbortHandler : AbortListener
		{
			private readonly AsyncTransaction parent;
			private readonly Txn txn;

			public AbortHandler(AsyncTransaction parent, Txn txn)
			{
				this.parent = parent;
				this.txn = txn;
			}

			public void OnSuccess(AbortStatus.AbortStatusType status)
			{
				parent.console.Error("Txn aborted: " + txn.Id);
				parent.NotifyComplete();
			}
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyComplete()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}
}
