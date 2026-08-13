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

public sealed class DocumentSindex : SyncExample
{
	internal const string SetName = "table1";
	internal const string NameIndex = "test_name_idx";
	internal const string TransactionIndex = "test_transaction_idx";
	internal const string NameBin = "name";
	internal const string TransactionBin = "transaction";

	public override void RunExample()
	{
		WriteTransactions();
		QueryDedicatedBin();
		QueryDocumentValues();
	}

	private void WriteTransactions()
	{
		List<Dictionary<string, object>> transactions =
		[
			new() { ["txn_id"] = "1111", ["name"] = "Davis", ["item_id"] = "A1234", ["count"] = 1 },
			new() { ["txn_id"] = "2222", ["name"] = "Johnson", ["item_id"] = "B2345", ["count"] = 2 },
			new() { ["txn_id"] = "3333", ["name"] = "Johnson", ["item_id"] = "C3456", ["count"] = 2 },
			new() { ["txn_id"] = "4444", ["name"] = "Lee", ["item_id"] = "D4567", ["count"] = 3 }
		];

		WritePolicy documentWritePolicy = new(writePolicy)
		{
			sendKey = true
		};

		foreach (Dictionary<string, object> transaction in transactions)
		{
			string userKey = transaction["txn_id"].ToString();
			Key key = new(ns, SetName, userKey);
			Bin nameBin = new(NameBin, transaction["name"]);
			Bin transactionBin = new(TransactionBin, transaction);

			try
			{
				client.Put(documentWritePolicy, key, nameBin, transactionBin);
				Record record = client.Get(null, key);
				console.Info($"Create succeeded\nKey: {key.userKey}\nRecord: {record}");
			}
			catch (AerospikeException ae)
			{
				console.Error($"Create failed for key {userKey}.", ae);
				throw;
			}
		}
	}

	private void QueryDedicatedBin()
	{
		client.CreateIndex(null, ns, SetName, NameIndex, NameBin, IndexType.STRING).Wait();

		Statement statement = new()
		{
			Namespace = ns,
			SetName = SetName,
			BinNames = [NameBin, TransactionBin],
			Filter = Filter.Equal(NameBin, "Johnson")
		};

		using RecordSet records = client.Query(null, statement);

		while (records.Next())
		{
			console.Info($"Key: {records.Key.userKey} | Record: {records.Record}");
		}
	}

	private void QueryDocumentValues()
	{
		client.CreateIndex(
			null,
			ns,
			SetName,
			TransactionIndex,
			TransactionBin,
			IndexType.STRING,
			IndexCollectionType.MAPVALUES).Wait();

		Statement statement = new()
		{
			Namespace = ns,
			SetName = SetName,
			BinNames = [NameBin, TransactionBin],
			Filter = Filter.Contains(TransactionBin, IndexCollectionType.MAPVALUES, "Johnson")
		};

		using RecordSet records = client.Query(null, statement);

		while (records.Next())
		{
			console.Info($"Key: {records.Key.userKey} | Record: {records.Record}");
		}
	}
}
