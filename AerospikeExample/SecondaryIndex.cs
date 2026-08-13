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

public sealed class SecondaryIndex : SyncExample
{
	internal const string SetName = "sindex-lifecycle";
	internal const string BinName = "occurred";
	internal const string IndexName = "occurred_idx";

	public override void RunExample()
	{
		Policy indexPolicy = new()
		{
			// Bounds the create command, each status poll, and the Wait() deadline.
			socketTimeout = 60000
		};

		CreateIndex(indexPolicy);
		DropIndex(indexPolicy);
	}

	private void CreateIndex(Policy indexPolicy)
	{
		try
		{
			IndexTask task = client.CreateIndex(
				indexPolicy,
				ns,
				SetName,
				IndexName,
				BinName,
				IndexType.NUMERIC);

			task.Wait();
			console.Info($"Created index {IndexName}");
		}
		catch (AerospikeException.Timeout ae)
		{
			// Wait() stopped polling. The server keeps building the index.
			console.Error($"Still waiting on index {IndexName}; the server is building it.", ae);
			throw;
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
		{
			console.Info($"Index {IndexName} already exists");
		}
	}

	private void DropIndex(Policy indexPolicy)
	{
		try
		{
			client.DropIndex(indexPolicy, ns, SetName, IndexName).Wait();
			console.Info($"Dropped index {IndexName}");
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_NOTFOUND)
		{
			console.Info($"Index {IndexName} does not exist");
		}
	}
}
