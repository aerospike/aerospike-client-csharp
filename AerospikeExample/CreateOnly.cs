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

public sealed class CreateOnly : SyncExample
{
	private const string MergeKey = "create-merge";
	private const string CreateKey = "create-only";

	public override void RunExample()
	{
		DemonstrateDefaultMerge();
		DemonstrateCreateOnly();
	}

	private void DemonstrateDefaultMerge()
	{
		Key key = new(ns, set, MergeKey);
		WritePolicy policy = new(writePolicy)
		{
			sendKey = true
		};

		client.Put(policy, key, new Bin("name", "Ada"), new Bin("language", "C#"));

		// UPDATE is the default RecordExistsAction. Only the supplied bin is changed,
		// so the existing "language" bin remains in the record.
		client.Put(policy, key, new Bin("name", "Grace"));
	}

	private void DemonstrateCreateOnly()
	{
		Key key = new(ns, set, CreateKey);

		WritePolicy createPolicy = new(writePolicy)
		{
			sendKey = true,
			recordExistsAction = RecordExistsAction.CREATE_ONLY,
			// Positive values are TTL seconds. 0 uses the namespace default-ttl,
			// -1 never expires, and -2 leaves an existing TTL unchanged.
			expiration = 60
		};

		try
		{
			client.Put(createPolicy, key, new Bin("status", "new"));
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.KEY_EXISTS_ERROR)
		{
			console.Info("The record was already created.");
		}
		catch (AerospikeException ae)
		{
			console.Error($"Create failed: {ResultCode.GetResultString(ae.Result)}", ae);
			throw;
		}

		// A second create demonstrates the expected duplicate result code.
		try
		{
			client.Put(createPolicy, key, new Bin("status", "duplicate"));
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.KEY_EXISTS_ERROR)
		{
			console.Info("Create-only correctly rejected the duplicate record.");
		}
	}
}
