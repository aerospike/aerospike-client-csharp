/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncBatch : TestAsync
	{
		private const string BinName = "batchbin";
		private const string BinName2 = "batchbin2";
		private const string BinName3 = "batchbin3";
		private const string ListBin = "listbin";
		private const string ListBin2 = "listbin2";
		private const string ListBin3 = "listbin3";
		private const string ValuePrefix = "batchvalue";
		private const int Size = 8;
		private static Key[] sendKeys;
		private static Key[] deleteKeys;
		private static Key[] deleteKeysSequence;
		private static readonly string[] binNotFound = ["binnotfound"];

		public static void WriteRecords(string keyPrefix)
		{
			sendKeys = new Key[Size];

			for (int i = 0; i < Size; i++)
			{
				sendKeys[i] = new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + (i + 1));
			}

			deleteKeys = new Key[2];
			deleteKeys[0] = new Key(SuiteHelpers.ns, SuiteHelpers.set, 10000);
			deleteKeys[1] = new Key(SuiteHelpers.ns, SuiteHelpers.set, 10001);

			deleteKeysSequence = new Key[2];
			deleteKeysSequence[0] = new Key(SuiteHelpers.ns, SuiteHelpers.set, 11000);
			deleteKeysSequence[1] = new Key(SuiteHelpers.ns, SuiteHelpers.set, 11001);

			AsyncMonitor monitor = new();
			WriteHandler handler = new(monitor, Size + 3);

			WritePolicy policy = new()
			{
				expiration = 2592000
			};

			for (int i = 1; i <= Size; i++)
			{
				Key key = sendKeys[i - 1];
				Bin bin = new(BinName, ValuePrefix + i);

				List<int> list = [];

				for (int j = 0; j < i; j++)
				{
					list.Add(j * i);
				}

				List<int> list2 = [];

				for (int j = 0; j < 2; j++)
				{
					list2.Add(j);
				}

				List<int> list3 = [];

				for (int j = 0; j < 2; j++)
				{
					list3.Add(j);
				}

				Bin listBin = new(ListBin, list);
				Bin listBin2 = new(ListBin2, list2);
				Bin listBin3 = new(ListBin3, list3);

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
			client.Put(policy, handler, new Key(SuiteHelpers.ns, SuiteHelpers.set, 10002), new Bin(BinName, 10002));
			client.Put(policy, handler, deleteKeysSequence[0], new Bin(BinName, 11000));
			client.Put(policy, handler, deleteKeysSequence[1], new Bin(BinName, 11001));

			monitor.WaitTillComplete();
		}

		private class WriteHandler(AsyncMonitor monitor, int max) : WriteListener
		{
			private int count;

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
			WriteRecords("AsyncBatchExistsArray");
			client.Exists(null, new ExistsArrayHandler(this), sendKeys);
			WaitTillComplete();
		}

		static void ExistsArrayHandlerSuccess(bool[] existsArray, TestAsyncBatch parent)
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

		private class ExistsArrayHandler(TestAsyncBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] existsArray)
			{
				ExistsArrayHandlerSuccess(existsArray, parent);
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
			WriteRecords("AsyncBatchExistsSequence");
			client.Exists(null, new ExistsSequenceHandler(this), sendKeys);
			WaitTillComplete();
		}

		private class ExistsSequenceHandler(TestAsyncBatch parent) : ExistsSequenceListener
		{
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
			WriteRecords("AsyncBatchGetArray");
			client.Get(null, new RecordArrayHandler(this), sendKeys);
			WaitTillComplete();
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

		private class RecordArrayHandler(TestAsyncBatch parent) : RecordArrayListener
		{
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
		public void AsyncBatchGetArrayBinName()
		{
			WriteRecords("AsyncBatchGetArrayBinName");
			client.Get(null, new GetArrayBinNameHandler(this), sendKeys, BinName);
			WaitTillComplete();
		}

		private class GetArrayBinNameHandler(TestAsyncBatch parent) : RecordArrayListener
		{
			public void OnSuccess(Key[] keys, Record[] records)
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
						parent.NotifyCompleted();
					}
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
			WriteRecords("AsyncBatchGetSequence");
			client.Get(null, new RecordSequenceHandler(this), sendKeys);
			WaitTillComplete();
		}

		private class RecordSequenceHandler(TestAsyncBatch parent) : RecordSequenceListener
		{
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
		public void AsyncBatchGetSequenceBinName()
		{
			WriteRecords("AsyncBatchGetSequenceBinName");
			client.Get(null, new GetSequenceBinNameHandler(this), sendKeys, BinName);
			WaitTillComplete();
		}

		private class GetSequenceBinNameHandler(TestAsyncBatch parent) : RecordSequenceListener
		{
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
			WriteRecords("AsyncBatchGetHeaders");
			client.GetHeader(null, new RecordHeaderArrayHandler(this), sendKeys);
			WaitTillComplete();
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

		private class RecordHeaderArrayHandler(TestAsyncBatch parent) : RecordArrayListener
		{
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
		public void AsyncBatchGetHeadersSeq()
		{
			WriteRecords("AsyncBatchGetHeadersBinName");
			client.GetHeader(null, new GetHeadersSeqHandler(sendKeys, this), sendKeys);
			WaitTillComplete();
		}

		private class GetHeadersSeqHandler(Key[] keys, TestAsyncBatch parent) : RecordSequenceListener
		{
			int count;
			
			public void OnRecord(Key key, Record record)
			{
				count++;

				int index = GetKeyIndex(key);
				
				if (!parent.AssertTrue(index >= 0))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertRecordFound(key, record))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertGreaterThanZero(record.generation))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertValidExpiration(record.expiration))
				{
					parent.NotifyCompleted();
					return;
				}
			}

			public void OnSuccess()
			{
				parent.AssertEquals(Size, count);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}

			private int GetKeyIndex(Key key)
			{
				for (int i = 0; i < keys.Length; i++)
				{
					if (key == keys[i])
					{
						return i;
					}
				}
				return -1;
			}
		}

		[TestMethod]
		public void AsyncBatchReadComplex()
		{
			string keyPrefix = "AsyncBatchReadComplex";
			WriteRecords(keyPrefix);
			// Batch gets into one call.
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

			// bin * 8
			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName), Exp.Val(8)));
			Operation[] ops = Operation.Array(ExpOperation.Read(BinName, exp, ExpReadFlags.DEFAULT));

			string[] bins = [BinName];
			List<BatchRead> records =
			[
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 1), bins),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 2), true),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 3), true),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 4), false),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 5), true),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 6), ops),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 7), bins),
				// This record should be found, but the requested bin will not be found.
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 8), binNotFound),
				// This record should not be found.
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, "keynotfound"), bins),
			];

			// Execute batch.
			client.Get(null, new BatchListHandler(this), records);
			WaitTillComplete();
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

		private class BatchListHandler(TestAsyncBatch parent) : BatchListListener
		{
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
		public void AsyncBatchReadComplexSeq()
		{
			string keyPrefix = "AsyncBatchReadComplexSeq";
			WriteRecords(keyPrefix);

			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName), Exp.Val(8)));
			Operation[] ops = Operation.Array(ExpOperation.Read(BinName, exp, ExpReadFlags.DEFAULT));
			
			string[] bins = [BinName];

			Key[] keys = 			[
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 1),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 2),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 3),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 4),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 5),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 6),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 7),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 8),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, "keyNotFound"),
			];

			List<BatchRead> records =
			[
				new BatchRead(keys[0], bins),
				new BatchRead(keys[1], true),
				new BatchRead(keys[2], true),
				new BatchRead(keys[3], false),
				new BatchRead(keys[4], true),
				new BatchRead(keys[5], ops),
				new BatchRead(keys[6], bins),
				// This record should be found, but the requested bin will not be found.
				new BatchRead(keys[7], binNotFound),
				// This record should not be found.
				new BatchRead(keys[8], bins),
			];

			// Execute batch.
			client.Get(null, new BatchReadComplexSeqHandler(this, keys), records);
			WaitTillComplete();
		}

		private class BatchReadComplexSeqHandler(TestAsyncBatch parent, Key[] keys) : BatchSequenceListener
		{
			private int found;
			private readonly Key[] keys = keys;

			public void OnRecord(BatchRead record)
			{
				Record rec = record.record;
				if (rec != null)
				{
					found++;
					int index = GetKeyIndex(record.key);

					if (!parent.AssertTrue(index >= 0))
					{
						parent.NotifyCompleted();
						return;
					}

					if (index != 3 && index != 5 && index <= 6)
					{
						object value = rec.GetValue(BinName);
						if (!parent.AssertEquals(ValuePrefix + (index + 1), value))
						{
							parent.NotifyCompleted();
							return;
						}
					}
					else if (index == 5)
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

			private int GetKeyIndex(Key key)
			{
				for (int i = 0; i < keys.Length; i++)
				{
					if (key == keys[i])
					{
						return i;
					}
				}
				return -1;
			}

			public void OnSuccess()
			{
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
			WriteRecords("AsyncBatchListReadOperate");
			Operation[] operations =
			[
				ListOperation.Size(ListBin),
				ListOperation.GetByIndex(ListBin, -1, ListReturnType.VALUE)
			];

			client.Get(null, new BatchListReadOperateHandler(this), sendKeys, operations);

			WaitTillComplete();
		}

		static void BatchListReadOperateHeadlerSuccess(Record[] records, TestAsyncBatch parent)
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

		private class BatchListReadOperateHandler(TestAsyncBatch parent) : RecordArrayListener
		{
			public void OnSuccess(Key[] keys, Record[] records)
			{
				BatchListReadOperateHeadlerSuccess(records, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchListReadOperateSeq()
		{
			WriteRecords("AsyncBatchListReadOperateSeq");
			client.Get(null, new BatchListReadOperateSeqHandler(this, sendKeys), sendKeys,
				ListOperation.Size(ListBin),
				ListOperation.GetByIndex(ListBin, -1, ListReturnType.VALUE));
			WaitTillComplete();
		}

		private class BatchListReadOperateSeqHandler(TestAsyncBatch parent, Key[] keys) : RecordSequenceListener
		{
			private int count = 0;
			private readonly Key[] keys = keys;

			public void OnRecord(Key key, Record record)
			{
				count++;

				int index = GetKeyIndex(key);

				if (!parent.AssertTrue(index >= 0))
				{
					parent.NotifyCompleted();
					return;
				}

				IList results = record.GetList(ListBin);
				long size = (long)results[0];
				long val = (long)results[1];

				if (!parent.AssertEquals(index + 1, size))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertEquals(index * (index + 1), val))
				{
					parent.NotifyCompleted();
					return;
				}
			}

			private int GetKeyIndex(Key key)
			{
				for (int i = 0; i < keys.Length; i++)
				{
					if (key == keys[i])
					{
						return i;
					}
				}
				return -1;
			}

			public void OnSuccess()
			{
				parent.AssertEquals(Size, count);
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
			WriteRecords("AsyncBatchListWriteOperate");
			Operation[] operations = [
				ListOperation.Insert(ListBin2, 0, Value.Get(1000)),
				ListOperation.Size(ListBin2),
				ListOperation.GetByIndex(ListBin2, -1, ListReturnType.VALUE)
			];

			client.Operate(null, null, new BatchListWriteOperateHandler(this), sendKeys,
					operations);

			WaitTillComplete();
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

		private class BatchListWriteOperateHandler(TestAsyncBatch parent) : BatchRecordArrayListener
		{
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
		public void AsyncBatchSeqListWriteOperate()
		{
			WriteRecords("AsyncBatchSeqListWriteOperate");
			Operation[] operations =
			[
				ListOperation.Insert(ListBin3, 0, Value.Get(1000)),
				ListOperation.Size(ListBin3),
				ListOperation.GetByIndex(ListBin3, -1, ListReturnType.VALUE)
			];

			client.Operate(null, null, new BatchSeqListWriteOperateHandler(this), sendKeys,
					operations);

			WaitTillComplete();
		}

		private class BatchSeqListWriteOperateHandler(TestAsyncBatch parent) : BatchRecordSequenceListener
		{
			private int count;

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
			string keyPrefix = "AsyncBatchWriteComplex";
			WriteRecords(keyPrefix);
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records =
			[
				new BatchWrite(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 1), ops1),
				new BatchWrite(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 6), ops2),
			];

			client.Operate(null, new BatchWriteComplexHandler(this), records);

			WaitTillComplete();
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

		private class BatchWriteComplexHandler(TestAsyncBatch parent) : BatchOperateListListener
		{
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
		public void AsyncBatchSeqWriteComplex()
		{
			string keyPrefix = "AsyncBatchSeqWriteComplex";
			WriteRecords(keyPrefix);
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] ops1 = Operation.Array(
				Operation.Put(new Bin(BinName2, 100)),
				Operation.Get(BinName2));

			Operation[] ops2 = Operation.Array(
				ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
				Operation.Get(BinName3));

			List<BatchRecord> records =
			[
				new BatchWrite(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 1), ops1),
				new BatchWrite(new Key(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + 6), ops2),
				new BatchDelete(new Key(SuiteHelpers.ns, SuiteHelpers.set, 10002)),
			];

			client.Operate(null, new BatchSeqWriteComplexHandler(this), records);

			WaitTillComplete();
		}

		private class BatchSeqWriteComplexHandler(TestAsyncBatch parent) : BatchRecordSequenceListener
		{
			private int count;

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
			WriteRecords("AsyncBatchDelete");
			// Ensure keys exists
			client.Exists(null, new BatchDeleteExistsArrayHandler(this), deleteKeys);

			WaitTillComplete();
		}

		static void BatchDeleteExistsArrayHandlerSuccess(Key[] keys, bool[] exists, TestAsyncBatch parent)
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
			client.Delete(null, null, new BatchDeleteHandler(parent), keys);
		}

		private class BatchDeleteExistsArrayHandler(TestAsyncBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] exists)
			{
				BatchDeleteExistsArrayHandlerSuccess(keys, exists, parent);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		static void BatchDeleteHandlerSuccess(bool status, TestAsyncBatch parent)
		{
			if (!parent.AssertEquals(true, status))
			{
				parent.NotifyCompleted();
				return;
			}

			// Ensure keys do not exist
			client.Exists(null, new NotExistsHandler(parent), deleteKeys);
		}

		private class BatchDeleteHandler(TestAsyncBatch parent) : BatchRecordArrayListener
		{
			public void OnSuccess(BatchRecord[] records, bool status)
			{
				BatchDeleteHandlerSuccess(status, parent);
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		static void NotExistsHandlerSuccess(bool[] exists, TestAsyncBatch parent)
		{
			if (!parent.AssertEquals(false, exists[0]))
			{
				parent.NotifyCompleted();
				return;
			}

			parent.AssertEquals(false, exists[1]);
			parent.NotifyCompleted();
		}

		private class NotExistsHandler(TestAsyncBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] exists)
			{
				NotExistsHandlerSuccess(exists, parent);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchDeleteSequence()
		{
			WriteRecords("AsyncBatchDeleteSequence");
			client.Exists(null, new BatchDeleteExistsSequenceHandler(this), deleteKeysSequence);
			WaitTillComplete();
		}

		private class BatchDeleteExistsSequenceHandler(TestAsyncBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] exists)
			{
				parent.AssertEquals(true, exists[0]);
				parent.AssertEquals(true, exists[1]);

				// Delete keys
				client.Delete(null, null, new BatchDeleteSequenceHandler(parent), deleteKeysSequence);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		private class BatchDeleteSequenceHandler(TestAsyncBatch parent) : BatchRecordSequenceListener
		{
			public void OnRecord(BatchRecord record, int index)
			{
				parent.AssertEquals(ResultCode.OK, record.resultCode);
				parent.AssertTrue(index <= 1);
			}

			public void OnSuccess()
			{
				client.Exists(null, new ExistsArrayDeleteSeqHandler(parent), deleteKeysSequence);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		private class ExistsArrayDeleteSeqHandler(TestAsyncBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] exists)
			{
				parent.AssertEquals(false, exists[0]);
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
