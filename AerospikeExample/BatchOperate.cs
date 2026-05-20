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
using System.Collections;

namespace Aerospike.Example;

public sealed class BatchOperate : SyncExample
{
	private const string KeyPrefix = "bkey";
	private const string BinName1 = "bin1";
	private const string BinName2 = "bin2";
	private const string BinName3 = "bin3";
	private const string BinName4 = "bin4";
	private const string ResultName1 = "result1";
	private const string ResultName2 = "result2";
	private const int RecordCount = 8;

	public override void RunExample()
	{
		BatchReadOperate();
		BatchReadOperateComplex();
		BatchListReadOperate();
		BatchListWriteOperate();
		BatchWriteOperateComplex();
	}

	/// <summary>
	/// Perform read operation expressions in one batch.
	/// </summary>
	private void BatchReadOperate()
	{
		console.Info(nameof(BatchReadOperate));

		Key[] keys = BuildKeys();
		Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
		Record[] records = client.Get(null, keys, ExpOperation.Read(ResultName1, exp, ExpReadFlags.DEFAULT));

		for (int i = 0; i < records.Length; i++)
		{
			Record record = records[i];

			if (record == null)
			{
				console.Info($"Result[{i}]: not found");
				continue;
			}

			console.Info($"Result[{i}]: {record.GetInt(ResultName1)}");
		}
	}

	/// <summary>
	/// Read results using varying read operations in one batch.
	/// </summary>
	private void BatchReadOperateComplex()
	{
		console.Info(nameof(BatchReadOperateComplex));

		Expression exp1 = Exp.Build(Exp.Mul(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
		Expression exp2 = Exp.Build(Exp.Add(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
		Expression exp3 = Exp.Build(Exp.Sub(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));

		// Batch uses pointer-reference equality to detect repeated operations and avoid
		// re-sending them. Allocating each operation array once preserves that.
		Operation[] ops1 = Operation.Array(ExpOperation.Read(ResultName1, exp1, ExpReadFlags.DEFAULT));
		Operation[] ops2 = Operation.Array(ExpOperation.Read(ResultName1, exp2, ExpReadFlags.DEFAULT));
		Operation[] ops3 = Operation.Array(ExpOperation.Read(ResultName1, exp3, ExpReadFlags.DEFAULT));
		Operation[] ops4 = Operation.Array(
			ExpOperation.Read(ResultName1, exp2, ExpReadFlags.DEFAULT),
			ExpOperation.Read(ResultName2, exp3, ExpReadFlags.DEFAULT));

		List<BatchRead> records =
		[
			new(new Key(ns, set, $"{KeyPrefix}1"), ops1),
			// Optimized: shares namespace, set, and ops references with the previous entry.
			new(new Key(ns, set, $"{KeyPrefix}2"), ops1),
			new(new Key(ns, set, $"{KeyPrefix}3"), ops2),
			new(new Key(ns, set, $"{KeyPrefix}4"), ops3),
			new(new Key(ns, set, $"{KeyPrefix}5"), ops4),
		];

		client.Get(null, records);
		PrintRecords(records);
	}

	/// <summary>
	/// Perform list read operations in one batch.
	/// </summary>
	private void BatchListReadOperate()
	{
		console.Info(nameof(BatchListReadOperate));

		Key[] keys = new Key[RecordCount];

		for (int i = 0; i < RecordCount; i++)
		{
			// Deliberately mis-key one entry to demonstrate graceful null handling.
			keys[i] = i == 5
				? new Key(ns, set, "not found")
				: new Key(ns, set, $"{KeyPrefix}{i + 1}");
		}

		Record[] records = client.Get(null, keys,
			ListOperation.Size(BinName3),
			ListOperation.GetByIndex(BinName3, -1, ListReturnType.VALUE));

		for (int i = 0; i < records.Length; i++)
		{
			Record record = records[i];

			if (record == null)
			{
				console.Info($"Result[{i}]: null");
				continue;
			}

			IList results = record.GetList(BinName3);
			console.Info($"Result[{i}]: {results[0]},{results[1]}");
		}
	}

	/// <summary>
	/// Perform list read/write operations in one batch.
	/// </summary>
	private void BatchListWriteOperate()
	{
		console.Info(nameof(BatchListWriteOperate));

		Key[] keys = BuildKeys();

		// Append integer to list and get size and last element of list bin for all records.
		BatchResults batch = client.Operate(null, null, keys,
			ListOperation.Append(ListPolicy.Default, BinName3, Value.Get(999)),
			ListOperation.Size(BinName3),
			ListOperation.GetByIndex(BinName3, -1, ListReturnType.VALUE));

		for (int i = 0; i < batch.records.Length; i++)
		{
			BatchRecord br = batch.records[i];

			if (br.resultCode == 0)
			{
				IList results = br.record.GetList(BinName3);
				console.Info($"Result[{i}]: {results[1]},{results[2]}");
			}
			else
			{
				console.Info($"Result[{i}]: error: {ResultCode.GetResultString(br.resultCode)}");
			}
		}
	}

	/// <summary>
	/// Read/write records using varying operations in one batch.
	/// </summary>
	private void BatchWriteOperateComplex()
	{
		console.Info(nameof(BatchWriteOperateComplex));

		Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName1), Exp.IntBin(BinName2), Exp.Val(1000)));
		Expression rexp1 = Exp.Build(Exp.Mul(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
		Expression rexp2 = Exp.Build(Exp.Add(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
		Expression rexp3 = Exp.Build(Exp.Sub(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));

		Operation[] ops1 = Operation.Array(
			Operation.Put(new Bin(BinName4, 100)),
			ExpOperation.Read(ResultName1, rexp1, ExpReadFlags.DEFAULT));
		Operation[] ops2 = Operation.Array(ExpOperation.Read(ResultName1, rexp1, ExpReadFlags.DEFAULT));
		Operation[] ops3 = Operation.Array(ExpOperation.Read(ResultName1, rexp2, ExpReadFlags.DEFAULT));
		Operation[] ops4 = Operation.Array(
			ExpOperation.Write(BinName1, wexp1, ExpWriteFlags.DEFAULT),
			ExpOperation.Read(ResultName1, rexp3, ExpReadFlags.DEFAULT));
		Operation[] ops5 = Operation.Array(
			ExpOperation.Read(ResultName1, rexp2, ExpReadFlags.DEFAULT),
			ExpOperation.Read(ResultName2, rexp3, ExpReadFlags.DEFAULT));

		List<BatchRecord> records =
		[
			new BatchWrite(new Key(ns, set, $"{KeyPrefix}1"), ops1),
			new BatchRead(new Key(ns, set, $"{KeyPrefix}2"), ops2),
			new BatchRead(new Key(ns, set, $"{KeyPrefix}3"), ops3),
			new BatchWrite(new Key(ns, set, $"{KeyPrefix}4"), ops4),
			new BatchRead(new Key(ns, set, $"{KeyPrefix}5"), ops5),
			new BatchDelete(new Key(ns, set, $"{KeyPrefix}6")),
		];

		client.Operate(null, records);

		for (int i = 0; i < records.Count; i++)
		{
			BatchRecord record = records[i];

			if (record.resultCode == 0)
			{
				object v1 = record.record?.GetValue(ResultName1) ?? "null";
				object v2 = record.record?.GetValue(ResultName2) ?? "null";
				console.Info($"Result[{i}]: {v1}, {v2}");
			}
			else
			{
				console.Info($"Result[{i}]: error: {ResultCode.GetResultString(record.resultCode)}");
			}
		}
	}

	private void PrintRecords(IList<BatchRead> records)
	{
		for (int i = 0; i < records.Count; i++)
		{
			BatchRead read = records[i];

			if (read.resultCode == 0 && read.record != null)
			{
				object v1 = read.record.GetValue(ResultName1);
				object v2 = read.record.GetValue(ResultName2);
				console.Info($"Result[{i}]: {v1}, {v2}");
			}
			else
			{
				console.Info($"Result[{i}]: error: {ResultCode.GetResultString(read.resultCode)}");
			}
		}
	}

	private Key[] BuildKeys()
	{
		Key[] keys = new Key[RecordCount];

		for (int i = 0; i < RecordCount; i++)
		{
			keys[i] = new Key(ns, set, $"{KeyPrefix}{i + 1}");
		}

		return keys;
	}
}
