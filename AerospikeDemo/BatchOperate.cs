/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Demo
{
	public class BatchOperate : SyncExample
	{
		private const string KeyPrefix = "bkey";
		private const string BinName1 = "bin1";
		private const string BinName2 = "bin2";
		private const string BinName3 = "bin3";
		private const string BinName4 = "bin4";
		private const string ResultName1 = "result1";
		private const string ResultName2 = "result2";
		private const int RecordCount = 8;

		public BatchOperate(Console console) : base(console)
		{
		}

		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			WriteRecords(client, args);
			BatchReadOperate(client, args);
			BatchReadOperateComplex(client, args);
			BatchListReadOperate(client, args);
			BatchListWriteOperate(client, args);
			BatchWriteOperateComplex(client, args);
		}

		private void WriteRecords(IAerospikeClient client, Arguments args)
		{
			for (int i = 1; i <= RecordCount; i++)
			{
				Key key = new Key(args.ns, args.set, KeyPrefix + i);
				Bin bin1 = new Bin(BinName1, i);
				Bin bin2 = new Bin(BinName2, i + 10);

				List<int> list = new List<int>();

				for (int j = 0; j < i; j++)
				{
					list.Add(j * i);
				}
				Bin bin3 = new Bin(BinName3, list);

				console.Info("Put: ns={0} set={1} key={2} val1={3} val2={4} val3={5}",
					key.ns, key.setName, key.userKey, bin1.value, bin2.value, Util.ListToString(list));

				client.Put(args.writePolicy, key, bin1, bin2, bin3);
			}
		}

		/// <summary>
		/// Perform read operation expressions in one batch.
		/// </summary>
		private void BatchReadOperate(IAerospikeClient client, Arguments args)
		{
			console.Info("batchReadOperate");
			Key[] keys = new Key[RecordCount];
			for (int i = 0; i < RecordCount; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			// bin1 * bin2
			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));

			Record[] records = client.Get(null, keys, ExpOperation.Read(ResultName1, exp, ExpReadFlags.DEFAULT));

			for (int i = 0; i < records.Length; i++)
			{
				Record record = records[i];
				console.Info("Result[{0}]: {1}", i, record.GetInt(ResultName1));
			}
		}

		/// <summary>
		/// Read results using varying read operations in one batch.
		/// </summary>
		private void BatchReadOperateComplex(IAerospikeClient client, Arguments args)
		{
			console.Info("batchReadOperateComplex");
			Expression exp1 = Exp.Build(Exp.Mul(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
			Expression exp2 = Exp.Build(Exp.Add(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));
			Expression exp3 = Exp.Build(Exp.Sub(Exp.IntBin(BinName1), Exp.IntBin(BinName2)));

			// Batch uses pointer reference to quickly determine if operations are repeated and can therefore
			// be optimized, but using varargs directly always creates a new reference. Therefore, save operation
			// array so we have one pointer reference per operation array.
			Operation[] ops1 = Operation.Array(ExpOperation.Read(ResultName1, exp1, ExpReadFlags.DEFAULT));
			Operation[] ops2 = Operation.Array(ExpOperation.Read(ResultName1, exp2, ExpReadFlags.DEFAULT));
			Operation[] ops3 = Operation.Array(ExpOperation.Read(ResultName1, exp3, ExpReadFlags.DEFAULT));
			Operation[] ops4 = Operation.Array(ExpOperation.Read(ResultName1, exp2, ExpReadFlags.DEFAULT),
											   ExpOperation.Read(ResultName2, exp3, ExpReadFlags.DEFAULT));

			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), ops1));
			// The following record is optimized (namespace,set,ops are only sent once) because
			// namespace, set and ops all have the same pointer references as the previous entry.
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), ops1));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), ops2));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 4), ops3));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), ops4));

			// Execute batch.
			client.Get(null, records);

			// Show results.
			int count = 0;
			foreach (BatchRead record in records)
			{
				if (record.resultCode == 0)
				{
					Record rec = record.record;
					object v1 = rec.GetValue(ResultName1);
					object v2 = rec.GetValue(ResultName2);
					console.Info("Result[{0}]: {1}, {2}", count++, v1, v2);
				}
				else
				{
					console.Info("Result[{0}]: error: {1}", count++, ResultCode.GetResultString(record.resultCode));
				}
			}
		}

		/// <summary>
		/// Perform list read operations in one batch.
		/// </summary>
		private void BatchListReadOperate(IAerospikeClient client, Arguments args)
		{
			console.Info("batchListReadOperate");
			Key[] keys = new Key[RecordCount];
			for (int i = 0; i < RecordCount; i++)
			{
				if (i == 5)
				{
					keys[i] = new Key(args.ns, args.set, "not found");
					continue;
				}
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			// Get size and last element of list bin for all records.
			Record[] records = client.Get(null, keys,
				ListOperation.Size(BinName3),
				ListOperation.GetByIndex(BinName3, -1, ListReturnType.VALUE));

			for (int i = 0; i < records.Length; i++)
			{
				Record record = records[i];

				if (record != null)
				{
					IList results = record.GetList(BinName3);
					long size = (long)results[0];
					object val = results[1];
					console.Info("Result[{0}]: {1},{2}", i, size, val);
				}
				else
				{
					console.Info("Result[{0}]: null", i);
				}
			}
		}

		/// <summary>
		/// Perform list read/write operations in one batch.
		/// </summary>
		private void BatchListWriteOperate(IAerospikeClient client, Arguments args)
		{
			console.Info("batchListWriteOperate");
			Key[] keys = new Key[RecordCount];
			for (int i = 0; i < RecordCount; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			// Add integer to list and get size and last element of list bin for all records.
			BatchResults bresults = client.Operate(null, null, keys,
				ListOperation.Append(ListPolicy.Default, BinName3, Value.Get(999)),
				ListOperation.Size(BinName3), ListOperation.GetByIndex(BinName3, -1, ListReturnType.VALUE));

			for (int i = 0; i < bresults.records.Length; i++)
			{
				BatchRecord br = bresults.records[i];

				if (br.resultCode == 0)
				{
					Record rec = br.record;
					IList results = rec.GetList(BinName3);
					long size = (long)results[1];
					object val = results[2];

					console.Info("Result[{0}]: {1},{2}", i, size, val);
				}
				else
				{
					console.Info("Result[{0}]: error: {1}", i, ResultCode.GetResultString(br.resultCode));
				}
			}
		}

		/// <summary>
		/// Read/Write records using varying operations in one batch.
		/// </summary>
		private void BatchWriteOperateComplex(IAerospikeClient client, Arguments args)
		{
			console.Info("batchWriteOperateComplex");
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

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 1), ops1));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), ops2));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), ops3));
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 4), ops4));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), ops5));
			records.Add(new BatchDelete(new Key(args.ns, args.set, KeyPrefix + 6)));

			// Execute batch.
			client.Operate(null, records);

			// Show results.
			int i = 0;
			foreach (BatchRecord record in records)
			{
				if (record.resultCode == 0)
				{
					Record rec = record.record;
					object v1 = rec.GetValue(ResultName1);
					object v2 = rec.GetValue(ResultName2);

					if (v1 == null)
					{
						v1 = "null";
					}

					if (v2 == null)
					{
						v2 = "null";
					}
					console.Info("Result[{0}]: {1}, {2}", i, v1, v2);
				}
				else
				{
					console.Info("Result[{0}]: error: {1}", i, ResultCode.GetResultString(record.resultCode));
				}
				i++;
			}
		}
	}
}
