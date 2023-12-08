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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncBatch : TestAsync
	{
		private const string BinName = "bbin";
		private const string BinName2 = "bbin2";
		private const string BinName3 = "bbin3";
		private const string ListBin = "lbin";
		private const string ListBin2 = "lbin2";
		private const string ListBin3 = "lbin3";
		private const string ValuePrefix = "batchvalue";
		private const int Size = 8;
		private static Key[] sendKeys;
		private static Key[] deleteKeys;
		private static CancellationTokenSource tokenSource;

		public static async Task WriteRecords(string keyPrefix)
		{
			tokenSource = new CancellationTokenSource();
			sendKeys = new Key[Size];

			for (int i = 0; i < Size; i++)
			{
				sendKeys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			deleteKeys = new Key[2];
			deleteKeys[0] = new Key(args.ns, args.set, 10000);
			deleteKeys[1] = new Key(args.ns, args.set, 10001);

			AsyncMonitor monitor = new AsyncMonitor();
			WriteHandler handler = new WriteHandler(monitor, Size + 3);

			WritePolicy policy = new WritePolicy();
			if (!args.testProxy || (args.testProxy && nativeClient != null))
			{
				policy.expiration = 2592000;
			}
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			for (int i = 1; i <= Size; i++)
			{
				Key key = sendKeys[i - 1];
				Bin bin = new Bin(BinName, ValuePrefix + i);

				List<int> list = new List<int>();

				for (int j = 0; j < i; j++)
				{
					list.Add(j * i);
				}

				List<int> list2 = new List<int>();

				for (int j = 0; j < 2; j++)
				{
					list2.Add(j);
				}

				List<int> list3 = new List<int>();

				for (int j = 0; j < 2; j++)
				{
					list3.Add(j);
				}

				Bin listBin = new Bin(ListBin, list);
				Bin listBin2 = new Bin(ListBin2, list2);
				Bin listBin3 = new Bin(ListBin3, list3);

				if (i != 6)
				{
					if (!args.testProxy)
					{
						client.Put(policy, handler, key, bin, listBin, listBin2, listBin3);
					}
					else
					{
						await client.Put(policy, tokenSource.Token, key, listBin, listBin2, listBin3);
					}
				}
				else
				{
					if (!args.testProxy)
					{
						client.Put(policy, handler, key, new Bin(BinName, i), listBin, listBin2, listBin3);
					}
					else
					{
						await client.Put(policy, tokenSource.Token, key, new Bin(BinName, i), listBin, listBin2, listBin3);
					}
				}
			}

			if (!args.testProxy)
			{
				// Add records that will eventually be deleted.
				client.Put(policy, handler, deleteKeys[0], new Bin(BinName, 10000));
				client.Put(policy, handler, deleteKeys[1], new Bin(BinName, 10001));
				client.Put(policy, handler, new Key(args.ns, args.set, 10002), new Bin(BinName, 10002));

				monitor.WaitTillComplete();
			}
			else
			{
				// Add records that will eventually be deleted.
				await client.Put(policy, tokenSource.Token, deleteKeys[0], new Bin(BinName, 10000));
				await client.Put(policy, tokenSource.Token, deleteKeys[1], new Bin(BinName, 10001));
				await client.Put(policy, tokenSource.Token, new Key(args.ns, args.set, 10002), new Bin(BinName, 10002));
			}
		}

		private class WriteHandler : WriteListener
		{
			private readonly AsyncMonitor monitor;
			private readonly int max;
			private int count;

			public WriteHandler(AsyncMonitor monitor, int max)
			{
				this.monitor = monitor;
				this.max = max;
			}

			public void OnSuccess(Key key)
			{
				int rows = Interlocked.Increment(ref count);

				if (rows == max)
				{
					monitor.NotifyCompleted();
				}
			}

			public void OnFailure(AerospikeException e)
			{
				monitor.SetError(e);
				monitor.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchExistsArray()
		{
			await WriteRecords("AsyncBatchExistsArray");
			if (!args.testProxy)
			{
				client.Exists(null, new ExistsArrayHandler(this), sendKeys);
				WaitTillComplete();
			}
			else
			{
				var exists = await client.Exists(null, tokenSource.Token, sendKeys);
				ExistsArrayHandlerSuccess(sendKeys, exists, this);
			}
		}

		static void ExistsArrayHandlerSuccess(Key[] keys, bool[] existsArray, TestAsyncBatch parent)
		{
			for (int i = 0; i < existsArray.Length; i++)
			{
				if (!parent.AssertEquals(true, existsArray[i]))
				{
					break;
				}
			}
			parent.NotifyCompleted();
		}

		private class ExistsArrayHandler : ExistsArrayListener
		{
			private readonly TestAsyncBatch parent;

			public ExistsArrayHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, bool[] existsArray)
			{
				ExistsArrayHandlerSuccess(keys, existsArray, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchExistsSequence()
		{
			await WriteRecords("AsyncBatchExistsSequence");
			if (!args.testProxy)
			{
				client.Exists(null, new ExistsSequenceHandler(this), sendKeys);
				WaitTillComplete();
			}
			else
			{
				var exists = await client.Exists(null, tokenSource.Token, sendKeys);
				foreach (bool exist in exists)
				{
					Assert.IsTrue(exist);
				}
			}
		}

		private class ExistsSequenceHandler : ExistsSequenceListener
		{
			private readonly TestAsyncBatch parent;

			public ExistsSequenceHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnExists(Key key, bool exists)
			{
				parent.AssertEquals(true, exists);
			}

			public void OnSuccess()
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchGetArray()
		{
			await WriteRecords("AsyncBatchGetArray");
			if (!args.testProxy)
			{
				client.Get(null, new RecordArrayHandler(this), sendKeys);
				WaitTillComplete();
			}
			else
			{
				var records = await client.Get(null, tokenSource.Token, sendKeys);
				RecordArrayHandlerSuccess(sendKeys, records, this);
			}
		}

		static void RecordArrayHandlerSuccess(Key[] keys, Record[] records, TestAsyncBatch parent)
		{
			try
			{
				if (parent.AssertEquals(Size, records.Length))
				{
					for (int i = 0; i < records.Length; i++)
					{
						if (i != 5)
						{
							if (!parent.AssertBinEqual(keys[i], records[i], BinName, ValuePrefix + (i + 1)))
							{
								break;
							}
						}
						else
						{
							if (!parent.AssertBinEqual(keys[i], records[i], BinName, i + 1))
							{
								break;
							}
						}
					}
				}
			}
			catch (Exception e)
			{
				parent.SetError(e);
			}
			finally
			{
				parent.NotifyCompleted();
			}
		}

		private class RecordArrayHandler : RecordArrayListener
		{
			private readonly TestAsyncBatch parent;

			public RecordArrayHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				RecordArrayHandlerSuccess(keys, records, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchGetSequence()
		{
			await WriteRecords("AsyncBatchGetSequence");
			if (!args.testProxy)
			{
				client.Get(null, new RecordSequenceHandler(this), sendKeys);
				WaitTillComplete();
			}
			else
			{
				var records = await client.Get(null, tokenSource.Token, sendKeys);
				foreach (Record record in records)
				{
					Object value = record.GetValue(BinName);
					AssertNotNull(value);
				}
			}
		}

		private class RecordSequenceHandler : RecordSequenceListener
		{
			private readonly TestAsyncBatch parent;

			public RecordSequenceHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnRecord(Key key, Record record)
			{
				if (parent.AssertRecordFound(key, record))
				{
					Object value = record.GetValue(BinName);
					parent.AssertNotNull(value);
				}
			}

			public void OnSuccess()
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchGetHeaders()
		{
			await WriteRecords("AsyncBatchGetHeaders");
			if (!args.testProxy)
			{
				client.GetHeader(null, new RecordHeaderArrayHandler(this), sendKeys);
				WaitTillComplete();
			}
			else
			{
				var records = await client.GetHeader(null, tokenSource.Token, sendKeys);
				RecordHeaderArrayHandlerSuccess(sendKeys, records, this);
			}
		}

		static void RecordHeaderArrayHandlerSuccess(Key[] keys, Record[] records, TestAsyncBatch parent)
		{
			if (parent.AssertEquals(Size, records.Length))
			{
				for (int i = 0; i < records.Length; i++)
				{
					Record record = records[i];

					if (!parent.AssertRecordFound(keys[i], record))
					{
						break;
					}

					if (!parent.AssertGreaterThanZero(record.generation))
					{
						break;
					}

					if (!parent.AssertGreaterThanZero(record.expiration))
					{
						break;
					}
				}
			}
			parent.NotifyCompleted();
		}

		private class RecordHeaderArrayHandler : RecordArrayListener
		{
			private readonly TestAsyncBatch parent;

			public RecordHeaderArrayHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				RecordHeaderArrayHandlerSuccess(keys, records, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchReadComplex()
		{
			string keyPrefix = "AsyncBatchReadComplex";
			await WriteRecords(keyPrefix);
			// Batch gets into one call.
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

			// bin * 8
			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName), Exp.Val(8)));
			Operation[] ops = Operation.Array(ExpOperation.Read(BinName, exp, ExpReadFlags.DEFAULT));

			string[] bins = new string[] { BinName };
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 6), ops));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 8), new string[] { "binnotfound" }));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			if (!args.testProxy)
			{
				client.Get(null, new BatchListHandler(this), records);
				WaitTillComplete();
			}
			else
			{
				var recordsReturned = await client.Get(null, tokenSource.Token, records);
				BatchListHandlerSuccess(recordsReturned, this);
			}
		}

		static void BatchListHandlerSuccess(List<BatchRead> records, TestAsyncBatch parent)
		{
			int found = 0;
			int count = 0;
			foreach (BatchRead record in records)
			{
				Record rec = record.record;
				count++;

				if (rec != null)
				{
					found++;

					if (count != 4 && count != 6 && count <= 7)
					{
						object value = rec.GetValue(BinName);

						if (!parent.AssertEquals(ValuePrefix + count, value))
						{
							parent.NotifyCompleted();
							return;
						}
					}
					else if (count == 6)
					{
						int value = rec.GetInt(BinName);

						if (!parent.AssertEquals(48, value))
						{
							parent.NotifyCompleted();
							return;
						}
					}
					else
					{
						Object value = rec.GetValue(BinName);

						if (!parent.AssertNull(value))
						{
							parent.NotifyCompleted();
							return;
						}
					}
				}
			}
			parent.AssertEquals(8, found);
			parent.NotifyCompleted();
		}

		private class BatchListHandler : BatchListListener
		{
			private readonly TestAsyncBatch parent;

			public BatchListHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(List<BatchRead> records)
			{
				BatchListHandlerSuccess(records, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchListReadOperate()
		{
			await WriteRecords("AsyncBatchListReadOperate");
			Operation[] operations =
			{
				ListOperation.Size(ListBin),
				ListOperation.GetByIndex(ListBin, -1, ListReturnType.VALUE)
			};

			if (!args.testProxy)
			{
				client.Get(null, new BatchListReadOperateHandler(this), sendKeys, operations);

				WaitTillComplete();
			}
			else
			{
				var records = await client.Get(null, tokenSource.Token, sendKeys, operations);
				BatchListReadOperateHeadlerSuccess(sendKeys, records, this);
			}
		}

		static void BatchListReadOperateHeadlerSuccess(Key[] keys, Record[] records, TestAsyncBatch parent)
		{
			if (parent.AssertEquals(Size, records.Length))
			{
				for (int i = 0; i < records.Length; i++)
				{
					Record record = records[i];
					IList results = record.GetList(ListBin);
					long size = (long)results[0];
					long val = (long)results[1];

					if (!parent.AssertEquals(i + 1, size))
					{
						break;
					}

					if (!parent.AssertEquals(i * (i + 1), val))
					{
						break;
					}
				}
			}
			parent.NotifyCompleted();
		}

		private class BatchListReadOperateHandler : RecordArrayListener
		{
			private readonly TestAsyncBatch parent;

			public BatchListReadOperateHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				BatchListReadOperateHeadlerSuccess(keys, records, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchListWriteOperate()
		{
			await WriteRecords("AsyncBatchListWriteOperate");
			Operation[] operations = {
				ListOperation.Insert(ListBin2, 0, Value.Get(1000)),
				ListOperation.Size(ListBin2),
				ListOperation.GetByIndex(ListBin2, -1, ListReturnType.VALUE)
			};


			if (!args.testProxy)
			{
				client.Operate(null, null, new BatchListWriteOperateHandler(this), sendKeys,
					operations);

				WaitTillComplete();
			}
			else
			{
				var results = await client.Operate(null, null, tokenSource.Token, sendKeys, operations);
				BatchListWriteOperateHandlerSuccess(results.records, results.status, this);
			}
		}

		static void BatchListWriteOperateHandlerSuccess(BatchRecord[] records, bool status, TestAsyncBatch parent)
		{
			parent.AssertEquals(true, status);

			if (parent.AssertEquals(Size, records.Length))
			{
				for (int i = 0; i < records.Length; i++)
				{
					Record record = records[i].record;
					IList results = record.GetList(ListBin2);
					long size = (long)results[1];
					long val = (long)results[2];

					if (!parent.AssertEquals(3, size))
					{
						break;
					}

					if (!parent.AssertEquals(1, val))
					{
						break;
					}
				}
			}
			parent.NotifyCompleted();
		}

		private class BatchListWriteOperateHandler : BatchRecordArrayListener
		{
			private readonly TestAsyncBatch parent;

			public BatchListWriteOperateHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				BatchListWriteOperateHandlerSuccess(records, status, parent);
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchSeqListWriteOperate()
		{
			await WriteRecords("AsyncBatchSeqListWriteOperate");
			Operation[] operations =
			{
				ListOperation.Insert(ListBin3, 0, Value.Get(1000)),
				ListOperation.Size(ListBin3),
				ListOperation.GetByIndex(ListBin3, -1, ListReturnType.VALUE)
			};

			if (!args.testProxy)
			{
				client.Operate(null, null, new BatchSeqListWriteOperateHandler(this), sendKeys,
					operations);

				WaitTillComplete();
			}
			else
			{
				var result = await client.Operate(null, null, tokenSource.Token, sendKeys, operations);
				foreach (BatchRecord batchRecord in result.records)
				{
					var record = batchRecord.record;
					if (AssertNotNull(record))
					{
						IList results = record.GetList(ListBin3);
						long size = (long)results[1];
						long val = (long)results[2];

						AssertEquals(3, size);
						AssertEquals(1, val);
					}
				}
				AssertEquals(8, result.records.Length);
			}
		}

		private class BatchSeqListWriteOperateHandler : BatchRecordSequenceListener
		{
			private readonly TestAsyncBatch parent;
			private int count;

			public BatchSeqListWriteOperateHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnRecord(BatchRecord record, int index)
			{
				Record rec = record.record;

				if (parent.AssertNotNull(rec))
				{
					IList results = rec.GetList(ListBin3);
					long size = (long)results[1];
					long val = (long)results[2];

					parent.AssertEquals(3, size);
					parent.AssertEquals(1, val);
					count++;
				}
			}

			public void OnSuccess()
			{
				parent.AssertEquals(Size, count);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchWriteComplex()
		{
			string keyPrefix = "AsyncBatchWriteComplex";
			await WriteRecords(keyPrefix);
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(new BatchWrite(new Key(args.ns, args.set, keyPrefix + 1), ops1));
			records.Add(new BatchWrite(new Key(args.ns, args.set, keyPrefix + 6), ops2));


			if (!args.testProxy)
			{
				client.Operate(null, new BatchWriteComplexHandler(this, records), records);

				WaitTillComplete();
			}
			else
			{
				var status = await client.Operate(null, tokenSource.Token, records);
				BatchWriteComplexHanlderSuccess(records, status, this);
			}
		}

		static void BatchWriteComplexHanlderSuccess(List<BatchRecord> records, bool status, TestAsyncBatch parent)
		{
			parent.AssertEquals(true, status);

			BatchRecord r = records[0];
			parent.AssertBatchBinEqual(r, BinName2, 100);

			r = records[1];
			parent.AssertBatchBinEqual(r, BinName3, 1006);

			parent.NotifyCompleted();
		}

		private class BatchWriteComplexHandler : BatchOperateListListener
		{
			private readonly TestAsyncBatch parent;
			private List<BatchRecord> records;

			public BatchWriteComplexHandler(TestAsyncBatch parent, List<BatchRecord> records)
			{
				this.parent = parent;
				this.records = records;
			}

			public void OnSuccess(List<BatchRecord> records, bool status)
			{
				BatchWriteComplexHanlderSuccess(records, status, parent);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncBatchSeqWriteComplex()
		{
			string keyPrefix = "AsyncBatchSeqWriteComplex";
			await WriteRecords(keyPrefix);
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(new BatchWrite(new Key(args.ns, args.set, keyPrefix + 1), ops1));
			records.Add(new BatchWrite(new Key(args.ns, args.set, keyPrefix + 6), ops2));
			records.Add(new BatchDelete(new Key(args.ns, args.set, 10002)));

			if (!args.testProxy)
			{
				client.Operate(null, new BatchSeqWriteComplexHandler(this), records);

				WaitTillComplete();
			}
			else
			{
				var status = await client.Operate(null, tokenSource.Token, records);
				AssertTrue(status);
			}
		}

		private class BatchSeqWriteComplexHandler : BatchRecordSequenceListener
		{
			private readonly TestAsyncBatch parent;
			private int count;

			public BatchSeqWriteComplexHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnRecord(BatchRecord r, int index)
			{
				count++;

				switch (index)
				{
					case 0:
						parent.AssertBatchBinEqual(r, BinName2, 100);
						break;

					case 1:
						parent.AssertBatchBinEqual(r, BinName3, 1006);
						break;

					case 2:
						parent.AssertEquals(ResultCode.OK, r.resultCode);
						break;

					default:
						parent.SetError(new Exception("Unexpected batch index: " + index));
						break;
				}
			}

			public void OnSuccess()
			{
				parent.AssertEquals(3, count);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		private bool AssertBatchBinEqual(BatchRecord r, String binName, int expected)
		{
			try
			{
				if (!AssertRecordFound(r.key, r.record))
				{
					return false;
				}

				IList list = r.record.GetList(binName);
				object obj = list[0];

				if (obj != null)
				{
					SetError(new Exception("Data mismatch: Expected null. Received " + obj));
					return false;
				}

				long val = (long)list[1];

				if (val != expected)
				{
					SetError(new Exception("Data mismatch: Expected " + expected + ". Received " + val));
					return false;
				}
				return true;
			}
			catch (Exception e)
			{
				SetError(new AerospikeException(e));
				return false;
			}
		}

		[TestMethod]
		public async Task AsyncBatchDelete()
		{
			await WriteRecords("AsyncBatchDelete");
			if (!args.testProxy)
			{
				// Ensure keys exists
				client.Exists(null, new BatchDeleteExistsArrayHandler(this), deleteKeys);

				WaitTillComplete();
			}
			else
			{
				var exists = await client.Exists(null, tokenSource.Token, deleteKeys);
				await BatchDeleteExistsArrayHandlerSuccess(sendKeys, exists, this);
			}
		}

		static async Task BatchDeleteExistsArrayHandlerSuccess(Key[] keys, bool[] exists, TestAsyncBatch parent)
		{
			if (!parent.AssertEquals(true, exists[0]))
			{
				parent.NotifyCompleted();
				return;
			}

			if (!parent.AssertEquals(true, exists[1]))
			{
				parent.NotifyCompleted();
				return;
			}

			// Delete keys
			if (!args.testProxy)
			{
				client.Delete(null, null, new BatchDeleteHandler(parent, keys, exists), keys);
			}
			else
			{
				var result = await client.Delete(null, null, tokenSource.Token, keys);
				await BatchDeleteHandlerSuccess(result.records, result.status, parent);
			}
		}

		private class BatchDeleteExistsArrayHandler : ExistsArrayListener
		{
			private readonly TestAsyncBatch parent;

			public BatchDeleteExistsArrayHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, bool[] exists)
			{
				BatchDeleteExistsArrayHandlerSuccess(keys, exists, parent).Wait();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		static async Task BatchDeleteHandlerSuccess(BatchRecord[] records, bool status, TestAsyncBatch parent)
		{
			if (!parent.AssertEquals(true, status))
			{
				parent.NotifyCompleted();
				return;
			}


			// Ensure keys do not exist
			if (!args.testProxy)
			{
				client.Exists(null, new NotExistsHandler(parent), deleteKeys);
			}
			else
			{
				var exists = await client.Exists(null, tokenSource.Token, deleteKeys);
				NotExistsHandlerSuccess(deleteKeys, exists, parent);
			}
		}

		private class BatchDeleteHandler : BatchRecordArrayListener
		{
			private readonly TestAsyncBatch parent;
			private Key[] keys;
			private bool[] exists;

			public BatchDeleteHandler(TestAsyncBatch parent, Key[] keys, bool[] exists)
			{
				this.parent = parent;
				this.keys = keys;
				this.exists = exists;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				BatchDeleteHandlerSuccess(records, status, parent).Wait();
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		static void NotExistsHandlerSuccess(Key[] keys, bool[] exists, TestAsyncBatch parent)
		{
			if (!parent.AssertEquals(false, exists[0]))
			{
				parent.NotifyCompleted();
				return;
			}

			parent.AssertEquals(false, exists[1]);
			parent.NotifyCompleted();
		}

		private class NotExistsHandler : ExistsArrayListener
		{
			private readonly TestAsyncBatch parent;

			public NotExistsHandler(TestAsyncBatch parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, bool[] exists)
			{
				NotExistsHandlerSuccess(keys, exists, parent);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}
	}
}
