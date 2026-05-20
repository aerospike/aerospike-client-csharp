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

public sealed class AsyncTransaction : AsyncExample
{
	private readonly ManualResetEventSlim completed = new();

	/// <summary>
	/// Run a read/write/delete transaction asynchronously using listener callbacks.
	/// </summary>
	public override void RunExample()
	{
		RequireEnterprise();
		RequireStrongConsistency();

		completed.Reset();

		using Txn txn = new();
		console.Info($"Begin txn: {txn.Id}");
		Put(txn);
		completed.Wait();
	}

	private void Put(Txn txn)
	{
		console.Info("Run put");
		WritePolicy wp = TxnWritePolicy(txn);
		Key key = new(ns, set, 1);

		client.Put(wp, new PutHandler(this, txn, key, txn1 => PutAnother(txn1)), key, new Bin("a", "val1"));
	}

	private void PutAnother(Txn txn)
	{
		console.Info("Run another put");
		WritePolicy wp = TxnWritePolicy(txn);
		Key key = new(ns, set, 2);

		client.Put(wp, new PutHandler(this, txn, key, txn1 => Get(txn1)), key, new Bin("b", "val2"));
	}

	private void Get(Txn txn)
	{
		console.Info("Run get");
		Policy p = new(client.ReadPolicyDefault)
		{
			Txn = txn
		};
		Key key = new(ns, set, 3);

		client.Get(p, new GetHandler(this, txn, key), key);
	}

	private void Delete(Txn txn)
	{
		console.Info("Run delete");
		WritePolicy dp = new(client.WritePolicyDefault)
		{
			Txn = txn,
			durableDelete = true // Required when running delete inside a transaction.
		};
		Key key = new(ns, set, 3);

		client.Delete(dp, new DeleteHandler(this, txn, key), key);
	}

	private void Commit(Txn txn)
	{
		console.Info("Run commit");
		client.Commit(new CommitHandler(this, txn), txn);
	}

	private void Abort(Txn txn)
	{
		console.Info("Run abort");
		client.Abort(new AbortHandler(this, txn), txn);
	}

	private void NotifyComplete() => completed.Set();

	private WritePolicy TxnWritePolicy(Txn txn) => new(client.WritePolicyDefault)
	{
		Txn = txn
	};

	private sealed class PutHandler(AsyncTransaction parent, Txn txn, Key key, Action<Txn> next) : WriteListener
	{
		public void OnSuccess(Key callbackKey) => next(txn);

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Failed to write: namespace={key.ns} set={key.setName} key={key.userKey} exception={e.Message}");
			parent.Abort(txn);
		}
	}

	private sealed class GetHandler(AsyncTransaction parent, Txn txn, Key key) : RecordListener
	{
		public void OnSuccess(Key callbackKey, Record record) => parent.Delete(txn);

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Failed to read: namespace={key.ns} set={key.setName} key={key.userKey} exception={e.Message}");
			parent.Abort(txn);
		}
	}

	private sealed class DeleteHandler(AsyncTransaction parent, Txn txn, Key key) : DeleteListener
	{
		public void OnSuccess(Key callbackKey, bool existed) => parent.Commit(txn);

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Failed to delete: namespace={key.ns} set={key.setName} key={key.userKey} exception={e.Message}");
			parent.Abort(txn);
		}
	}

	private sealed class CommitHandler(AsyncTransaction parent, Txn txn) : CommitListener
	{
		public void OnSuccess(CommitStatus.CommitStatusType status)
		{
			parent.console.Info($"Txn committed: {txn.Id}");
			parent.NotifyComplete();
		}

		public void OnFailure(AerospikeException.Commit ae)
		{
			parent.console.Error($"Txn commit failed: {txn.Id}");
			parent.NotifyComplete();
		}
	}

	private sealed class AbortHandler(AsyncTransaction parent, Txn txn) : AbortListener
	{
		public void OnSuccess(AbortStatus.AbortStatusType status)
		{
			parent.console.Error($"Txn aborted: {txn.Id}");
			parent.NotifyComplete();
		}
	}
}
