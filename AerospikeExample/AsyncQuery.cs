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

public class AsyncQuery(Console console) : AsyncExample(console)
{
	private bool completed;

	/// <summary>
	/// Asynchronous Query example.
	/// </summary>
	public override void RunExample(AsyncClient client, Arguments args)
	{
		completed = false;
		string indexName = "asqindex";
		string keyPrefix = "asqkey";
		string binName = args.GetBinName("asqbin");
		int size = 50;

		CreateIndex(client, args, indexName, binName);
		RunQueryExample(client, args, keyPrefix, binName, size);
		WaitTillComplete();

		var verifyAsyncQueryKey = new Key(args.ns, args.set, keyPrefix + 1);
		Record verifyAsyncQueryRecord = client.Get(null, verifyAsyncQueryKey);
		if (verifyAsyncQueryRecord == null)
		{
			throw new Exception("AsyncQuery verification failed: record not found for written key.");
		}
		console.Info("AsyncQuery verified successfully.");
		client.DropIndex(args.policy, args.ns, args.set, indexName);
	}

	private void CreateIndex(AsyncClient client, Arguments args, string indexName, string binName)
	{
		console.Info("Create index: ns=" + args.ns + " set=" + args.set + " index=" + indexName + " bin=" + binName);

		Policy policy = new()
		{
			totalTimeout = 0 // Do not timeout on index create.
		};

		try
		{
			client.DropIndex(policy, args.ns, args.set, indexName);
		}
		catch (AerospikeException)
		{
		}

		IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.INTEGER);
		task.Wait();
	}

	private void RunQueryExample(AsyncClient client, Arguments args, string keyPrefix, string binName, int size)
	{
		console.Info("Write " + size + " records.");
		var handler = new WriteHandler(this, client, args, binName, size);

		for (int i = 1; i <= size; i++)
		{
			var key = new Key(args.ns, args.set, keyPrefix + i);
			var bin = new Bin(binName, i);
			client.Put(args.writePolicy, handler, key, bin);
		}
	}

	private class WriteHandler(AsyncQuery parent, AsyncClient client, Arguments args, string binName, int max) : WriteListener
	{
		private readonly AsyncClient client = client;
		private readonly Arguments args = args;
		private readonly AsyncQuery parent = parent;
		private readonly string binName = binName;
		internal readonly int max = max;
		internal int count;

		public void OnSuccess(Key key)
		{
			int rows = Interlocked.Increment(ref count);

			if (rows == max)
			{
				int begin = 26;
				int end = 34;

				parent.console.Info("Query for: ns=" + args.ns + " set=" + args.set + " bin=" + binName + " >= " + begin + " <= " + end);

				Statement stmt = new();
				stmt.SetNamespace(args.ns);
				stmt.SetSetName(args.set);
				stmt.SetBinNames(binName);
				stmt.SetFilter(Filter.Range(binName, begin, end));

				QueryPolicy qp = new()
				{
					failOnClusterChange = true
				};

				client.Query(qp, new RecordSequenceHandler(parent, binName), stmt);
			}
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Put failed: " + e.Message);
			parent.NotifyCompleted();
		}
	}

	private class RecordSequenceHandler(AsyncQuery parent, string binName) : RecordSequenceListener
	{
		private readonly AsyncQuery parent = parent;
		private readonly string binName = binName;

		public void OnRecord(Key key, Record record)
		{
			int result = record.GetInt(binName);
			parent.console.Info("Result: " + result);
		}

		public void OnSuccess()
		{
			parent.NotifyCompleted();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Query failed: " + Util.GetErrorMessage(e));
			parent.NotifyCompleted();
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

	private void NotifyCompleted()
	{
		lock (this)
		{
			completed = true;
			Monitor.Pulse(this);
		}
	}
}
