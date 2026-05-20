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

public sealed class AsyncUserDefinedFunction : AsyncExample
{
	private readonly ManualResetEventSlim completed = new();

	/// <summary>
	/// Asynchronously execute a UDF and then read the resulting bin via a second UDF.
	/// </summary>
	public override void RunExample()
	{
		completed.Reset();
		WriteUsingUdfAsync();
		completed.Wait();
	}

	private void WriteUsingUdfAsync()
	{
		Key key = new(ns, set, "audfkey1");
		Bin bin = new("audfbin1", "string value");

		console.Info($"Write with udf: namespace={key.ns} set={key.setName} key={key.userKey} value={bin.value}");
		client.Execute(null, new WriteHandler(this, key, bin.name), key,
			"record_example", "writeBin", Value.Get(bin.name), bin.value);
	}

	private void NotifyCompleted() => completed.Set();

	private sealed class WriteHandler(AsyncUserDefinedFunction parent, Key key, string binName) : ExecuteListener
	{
		public void OnSuccess(Key callbackKey, object obj)
		{
			parent.console.Info($"Read with udf: namespace={callbackKey.ns} set={callbackKey.setName} key={callbackKey.userKey}");
			parent.client.Execute(null, new ReadHandler(parent), callbackKey,
				"record_example", "readBin", Value.Get(binName));
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Failed to execute: namespace={key.ns} set={key.setName} key={key.userKey} exception={e.Message}");
			parent.NotifyCompleted();
		}
	}

	private sealed class ReadHandler(AsyncUserDefinedFunction parent) : ExecuteListener
	{
		public void OnSuccess(Key key, object obj)
		{
			parent.console.Info($"Result: {obj}");
			parent.NotifyCompleted();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Failed to read: {e.Message}");
			parent.NotifyCompleted();
		}
	}
}
