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

/// <summary>
/// Demonstrate read patterns referenced from documentation snippets.
/// </summary>
public sealed class Get : SyncExample
{
	private const string DocReadKey = "docreadkey";

	public override void RunExample()
	{
		RunReadPolicy();
		RunExists();
		RunGetHeader();
		RunGetWholeRecord();
		RunGetSpecificBins();
	}

	private void RunReadPolicy()
	{
		// @@@SNIPSTART csharp-client-read-policy
		// Create new read policy
		Policy policy = new()
		{
			socketTimeout = 300
		};
		// @@@SNIPEND
		console.Info($"Read policy: socketTimeout={policy.socketTimeout}");
	}

	private void RunExists()
	{
		Key key = new(ns, set, DocReadKey);

		// @@@SNIPSTART csharp-client-read-exists
		// Returns true if exists, false if not
		bool exists = client.Exists(policy, key);

		// Do something
		console.Info($"Exists: {exists}");
		// @@@SNIPEND
	}

	private void RunGetHeader()
	{
		Key key = new(ns, set, DocReadKey);

		// @@@SNIPSTART csharp-client-read-metadata
		// Get record metadata
		Record record = client.GetHeader(policy, key);

		// Do something
		console.Info($"Record: {record}");
		// @@@SNIPEND
	}

	private void RunGetWholeRecord()
	{
		Key key = new(ns, set, DocReadKey);

		// @@@SNIPSTART csharp-client-read-whole-record
		// Get whole record
		Record record = client.Get(policy, key);

		// Do something
		console.Info($"Record: {record}");
		// @@@SNIPEND
	}

	private void RunGetSpecificBins()
	{
		Key key = new(ns, set, DocReadKey);

		// @@@SNIPSTART csharp-client-read-specific-bins
		// Get bins 'report' and 'location'
		Record record = client.Get(policy, key, "report", "location");

		// Do something
		console.Info($"Record: {record}");
		// @@@SNIPEND
	}
}
