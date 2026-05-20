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

public sealed class Batch : SyncExample
{
	private const string KeyPrefix = "batchkey";
	private const string BinName = "batchbin";
	private const int Size = 8;

	/// <summary>
	/// Batch multiple gets into a single call to the server.
	/// </summary>
	public override void RunExample()
	{
		BatchExists();
		BatchReads();
		BatchReadHeaders();
		BatchReadComplex();
	}

	/// <summary>
	/// Check existence of records in one batch.
	/// </summary>
	private void BatchExists()
	{
		Key[] keys = BuildKeys();
		bool[] existsArray = client.Exists(null, keys);

		for (int i = 0; i < existsArray.Length; i++)
		{
			Key key = keys[i];
			console.Info($"Record: namespace={key.ns} set={key.setName} key={key.userKey} exists={existsArray[i]}");
		}
	}

	/// <summary>
	/// Read records in one batch.
	/// </summary>
	private void BatchReads()
	{
		Key[] keys = BuildKeys();
		Record[] records = client.Get(null, keys, BinName);

		for (int i = 0; i < records.Length; i++)
		{
			Key key = keys[i];
			object value = records[i]?.GetValue(BinName);
			console.Info($"Record: namespace={key.ns} set={key.setName} key={key.userKey} bin={BinName} value={value}");
		}
	}

	/// <summary>
	/// Read record header data in one batch.
	/// </summary>
	private void BatchReadHeaders()
	{
		Key[] keys = BuildKeys();
		Record[] records = client.GetHeader(null, keys);

		for (int i = 0; i < records.Length; i++)
		{
			Key key = keys[i];
			Record record = records[i];
			console.Info(
				$"Record: namespace={key.ns} set={key.setName} key={key.userKey} " +
				$"generation={record?.generation} expiration={record?.expiration}");
		}
	}

	/// <summary>
	/// Read records with varying bin names and read types in one batch.
	/// </summary>
	private void BatchReadComplex()
	{
		// Batch allows multiple namespaces in one call, but the example environment
		// usually has only one namespace.
		string[] bins = [BinName];
		List<BatchRead> records =
		[
			new(new Key(ns, set, $"{KeyPrefix}1"), bins),
			new(new Key(ns, set, $"{KeyPrefix}2"), true),
			new(new Key(ns, set, $"{KeyPrefix}3"), true),
			new(new Key(ns, set, $"{KeyPrefix}4"), false),
			new(new Key(ns, set, $"{KeyPrefix}5"), true),
			new(new Key(ns, set, $"{KeyPrefix}6"), true),
			new(new Key(ns, set, $"{KeyPrefix}7"), bins),
			// This record exists, but the requested bin will not.
			new(new Key(ns, set, $"{KeyPrefix}8"), ["binnotfound"]),
			// This record will not be found.
			new(new Key(ns, set, "keynotfound"), bins),
		];

		client.Get(null, records);

		foreach (BatchRead read in records)
		{
			Key key = read.key;

			if (read.record != null)
			{
				console.Info($"Record: ns={key.ns} set={key.setName} key={key.userKey} bin={BinName} value={read.record.GetValue(BinName)}");
			}
			else
			{
				console.Info($"Record not found: ns={key.ns} set={key.setName} key={key.userKey} bin={BinName}");
			}
		}
	}

	private Key[] BuildKeys()
	{
		Key[] keys = new Key[Size];

		for (int i = 0; i < Size; i++)
		{
			keys[i] = new Key(ns, set, $"{KeyPrefix}{i + 1}");
		}

		return keys;
	}
}
