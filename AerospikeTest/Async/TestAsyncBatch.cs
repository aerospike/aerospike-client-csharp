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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

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
		private const string KeyPrefix = "asyncbatchkey";
		private const string ValuePrefix = "batchvalue";
		private const int Size = 8;
		private static Key[] sendKeys;
		private static Key[] deleteKeys;

		[ClassInitialize()]
		public static void WriteRecords(TestContext testContext)
		{
			sendKeys = new Key[Size];

			for (int i = 0; i < Size; i++)
			{
				sendKeys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			deleteKeys = new Key[2];
			deleteKeys[0] = new Key(args.ns, args.set, 10000);
			deleteKeys[1] = new Key(args.ns, args.set, 10001);

			AsyncMonitor monitor = new AsyncMonitor();
			WriteHandler handler = new WriteHandler(monitor, Size + 3);

			WritePolicy policy = new WritePolicy();
			policy.expiration = 2592000;

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
					client.Put(policy, handler, key, bin, listBin, listBin2, listBin3);
				}
				else
				{
					client.Put(policy, handler, key, new Bin(BinName, i), listBin, listBin2, listBin3);
				}
			}
			
			// Add records that will eventually be deleted.
			client.Put(policy, handler, deleteKeys[0], new Bin(BinName, 10000));
			client.Put(policy, handler, deleteKeys[1], new Bin(BinName, 10001));
			client.Put(policy, handler, new Key(args.ns, args.set, 10002), new Bin(BinName, 10002));

			if (!args.testProxy)
			{
				monitor.WaitTillComplete();
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
		public void AsyncBatchExistsArray()
		{
			client.Exists(null, new ExistsArrayHandler(this), sendKeys);
			WaitTillComplete();
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
				for (int i = 0; i < existsArray.Length; i++)
				{
					if (!parent.AssertEquals(true, existsArray[i]))
					{
						break;
					}
				}
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchExistsSequence()
		{
			client.Exists(null, new ExistsSequenceHandler(this), sendKeys);
			WaitTillComplete();
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
		public void AsyncBatchGetArray()
		{
			client.Get(null, new RecordArrayHandler(this), sendKeys);
			WaitTillComplete();
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

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchGetSequence()
		{
			client.Get(null, new RecordSequenceHandler(this), sendKeys);
			WaitTillComplete();
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
		public void AsyncBatchGetHeaders()
		{
			client.GetHeader(null, new RecordHeaderArrayHandler(this), sendKeys);
			WaitTillComplete();
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

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchReadComplex()
		{
			// Batch gets into one call.
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

			// bin * 8
			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName), Exp.Val(8)));
			Operation[] ops = Operation.Array(ExpOperation.Read(BinName, exp, ExpReadFlags.DEFAULT));

			string[] bins = new string[] { BinName };
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 6), ops));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 8), new string[] { "binnotfound" }));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			client.Get(null, new BatchListHandler(this), records);
			WaitTillComplete();
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
				int found = 0;
				int count = 0;
				foreach (BatchRead record in records)
				{
					Record rec = record.record;
					count++;
					
					if (rec != null) {
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

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchListReadOperate()
		{
			client.Get(null, new BatchListReadOperateHandler(this), sendKeys,
				ListOperation.Size(ListBin),
				ListOperation.GetByIndex(ListBin, -1, ListReturnType.VALUE));

			WaitTillComplete();
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

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchListWriteOperate()
		{
			client.Operate(null, null, new BatchListWriteOperateHandler(this), sendKeys,
				ListOperation.Insert(ListBin2, 0, Value.Get(1000)), ListOperation.Size(ListBin2),
				ListOperation.GetByIndex(ListBin2, -1, ListReturnType.VALUE));

			WaitTillComplete();
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

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchSeqListWriteOperate()
		{
			client.Operate(null, null, new BatchSeqListWriteOperateHandler(this), sendKeys,
				ListOperation.Insert(ListBin3, 0, Value.Get(1000)), ListOperation.Size(ListBin3), ListOperation.GetByIndex(ListBin3, -1, ListReturnType.VALUE));

			WaitTillComplete();
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
		public void AsyncBatchWriteComplex()
		{
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 1), ops1));
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 6), ops2));

			client.Operate(null, new BatchWriteComplexHandler(this, records), records);

			WaitTillComplete();
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
				parent.AssertEquals(true, status);

				BatchRecord r = records[0];
				parent.AssertBatchBinEqual(r, BinName2, 100);

				r = records[1];
				parent.AssertBatchBinEqual(r, BinName3, 1006);

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchSeqWriteComplex()
		{
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 1), ops1));
			records.Add(new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 6), ops2));
			records.Add(new BatchDelete(new Key(args.ns, args.set, 10002)));

			client.Operate(null, new BatchSeqWriteComplexHandler(this), records);

			WaitTillComplete();
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
		public void AsyncBatchDelete()
		{
			// Ensure keys exists
			client.Exists(null, new BatchDeleteExistsArrayHandler(this), deleteKeys);

			WaitTillComplete();
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
				client.Delete(null, null, new BatchDeleteHandler(parent, keys, exists), keys);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
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
				if (!parent.AssertEquals(true, status))
				{
					parent.NotifyCompleted();
					return;
				}

				// Ensure keys do not exist
				client.Exists(null, new NotExistsHandler(parent), deleteKeys);
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
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
				if (!parent.AssertEquals(false, exists[0]))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertEquals(false, exists[1]);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}
	}
}
