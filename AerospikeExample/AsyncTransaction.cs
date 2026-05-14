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

public class AsyncTransaction(Console console) : AsyncExample(console)
{
	private bool completed;

	/// <summary>
	/// Transaction.
	/// </summary>
	public override void RunExample(AsyncClient client, Arguments args)
	{
		RequireEnterprise(args);
		RequireStrongConsistency(args);

		completed = false;

		using Txn txn = new();

		console.Info("Begin txn: " + txn.Id);
		Put(client, txn, args);

		WaitTillComplete();

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

	public void Put(AsyncClient client, Txn txn, Arguments args)
	{
		console.Info("Run put");

		WritePolicy wp = new(client.WritePolicyDefault)
		{
			Txn = txn
		};

		Key key = new(args.ns, args.set, 1);

		client.Put(wp, new PutHandler(this, client, key, txn, args), key, new Bin("a", "val1"));
	}

	class PutHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args) : WriteListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly AsyncClient client = client;
		private readonly Key key = key;
		private readonly Txn txn = txn;
		private readonly Arguments args = args;

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

		WritePolicy wp = new(client.WritePolicyDefault)
		{
			Txn = txn
		};

		Key key = new(args.ns, args.set, 2);

		client.Put(wp, new PutAnotherHandler(this, client, key, txn, args), key, new Bin("b", "val2"));
	}

	class PutAnotherHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args) : WriteListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly AsyncClient client = client;
		private readonly Key key = key;
		private readonly Txn txn = txn;
		private readonly Arguments args = args;

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

		Policy p = new(client.ReadPolicyDefault)
		{
			Txn = txn
		};

		Key key = new(args.ns, args.set, 3);

		client.Get(p, new GetHandler(this, client, key, txn, args), key);
	}

	class GetHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn, Arguments args) : RecordListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly AsyncClient client = client;
		private readonly Key key = key;
		private readonly Txn txn = txn;
		private readonly Arguments args = args;

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

		WritePolicy dp = new(client.WritePolicyDefault)
		{
			Txn = txn,
			durableDelete = true  // Required when running delete in a transaction.
		};

		Key key = new(args.ns, args.set, 3);

		client.Delete(dp, new DeleteHandler(this, client, key, txn), key);
	}

	class DeleteHandler(AsyncTransaction parent, AsyncClient client, Key key, Txn txn) : DeleteListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly AsyncClient client = client;
		private readonly Key key = key;
		private readonly Txn txn = txn;

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

	class CommitHandler(AsyncTransaction parent, Txn txn) : CommitListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly Txn txn = txn;

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

	class AbortHandler(AsyncTransaction parent, Txn txn) : AbortListener
	{
		private readonly AsyncTransaction parent = parent;
		private readonly Txn txn = txn;

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
