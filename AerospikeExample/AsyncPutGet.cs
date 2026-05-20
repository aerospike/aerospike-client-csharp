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

public sealed class AsyncPutGet : AsyncExample
{
	private readonly ManualResetEventSlim completed = new();

	/// <summary>
	/// Asynchronously write and read a record, first using listener callbacks
	/// and then using Task-based async/await.
	/// </summary>
	public override void RunExample()
	{
		completed.Reset();

		Key key = new(ns, set, "putgetkey");
		Bin bin = new("bin1", "value");

		RunPutGetListener(key, bin);
		RunPutGetWithTask(key, bin).GetAwaiter().GetResult();
	}

	private void RunPutGetListener(Key key, Bin bin)
	{
		console.Info($"Put inline: namespace={key.ns} set={key.setName} key={key.userKey} value={bin.value}");

		client.Put(writePolicy, new WriteHandler(this, client, policy, bin), key, bin);
		completed.Wait();
	}

	private async Task RunPutGetWithTask(Key key, Bin bin)
	{
		console.Info($"Put with task: namespace={key.ns} set={key.setName} key={key.userKey} value={bin.value}");

		CancellationToken token = CancellationToken.None;
		await client.Put(writePolicy, token, key, bin);

		Record record = await client.Get(policy, token, key);
		ValidateBin(key, bin, record);
	}

	private void ValidateBin(Key key, Bin bin, Record record)
	{
		object received = record?.GetValue(bin.name);
		string expected = bin.value.ToString();

		if (received != null && received.Equals(expected))
		{
			console.Info(
				$"Bin matched: namespace={key.ns} set={key.setName} key={key.userKey} " +
				$"bin={bin.name} value={received} generation={record.generation} expiration={record.expiration}");
		}
		else
		{
			console.Error($"Put/Get mismatch: expected {expected}, received {received}");
		}
	}

	private void Complete()
	{
		completed.Set();
	}

	private sealed class WriteHandler(AsyncPutGet parent, IAsyncClient client, Policy policy, Bin bin)
		: WriteListener
	{
		public void OnSuccess(Key key)
		{
			try
			{
				parent.console.Info($"Get: namespace={key.ns} set={key.setName} key={key.userKey}");
				client.Get(policy, new RecordHandler(parent, bin), key);
			}
			catch (Exception ex)
			{
				parent.console.Error("Failed to read", ex);
				parent.Complete();
			}
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Failed to put", e);
			parent.Complete();
		}
	}

	private sealed class RecordHandler(AsyncPutGet parent, Bin bin) : RecordListener
	{
		public void OnSuccess(Key key, Record record)
		{
			parent.ValidateBin(key, bin, record);
			parent.Complete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error("Failed to get", e);
			parent.Complete();
		}
	}
}
