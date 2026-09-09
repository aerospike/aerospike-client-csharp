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

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncSingleBatch : TestAsync
	{
		private const string BinName = "asyncsbbin";
		private const string ValuePrefix = "asyncsbvalue";
		private static Key seedKey;

		[ClassInitialize]
		public static void SeedRecord(TestContext testContext)
		{
			AsyncMonitor monitor = new();
			seedKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "async-single-batch-seed");
			client.Put(null, new SeedWriteHandler(monitor), seedKey, new Bin(BinName, ValuePrefix));
			monitor.WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleGet()
		{
			Key[] keys = [seedKey];

			client.Get(null, new SingleGetArrayHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleGetSequence()
		{
			Key[] keys = [seedKey];

			client.Get(null, new SingleGetSequenceHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleGetHeader()
		{
			Key[] keys = [seedKey];

			client.GetHeader(null, new SingleGetHeaderArrayHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleGetHeaderSequence()
		{
			Key[] keys = [seedKey];

			client.GetHeader(null, new SingleGetHeaderSequenceHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleExists()
		{
			Key[] keys = [seedKey];

			client.Exists(null, new SingleExistsArrayHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleExistsSequence()
		{
			Key[] keys = [seedKey];

			client.Exists(null, new SingleExistsSequenceHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateRead()
		{
			List<BatchRecord> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			client.Operate(null, new SingleOperateListHandler(this), records);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateReadSequence()
		{
			List<BatchRecord> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			client.Operate(null, new SingleOperateSequenceHandler(this), records);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateWrite()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-single-batch-write");
			List<BatchRecord> records =
			[
				new BatchWrite(key, [Operation.Put(new Bin(BinName, ValuePrefix + "-write"))])
			];

			client.Operate(null, new SingleOperateWriteHandler(this, key), records);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateKeys()
		{
			Operation[] ops = [Operation.Get(BinName)];

			client.Operate(null, null, new SingleOperateKeysHandler(this), [seedKey], ops);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateKeysSequence()
		{
			Operation[] ops = [Operation.Get(BinName)];

			client.Operate(null, null, new SingleOperateKeysSequenceHandler(this), [seedKey], ops);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleDeleteNotFound()
		{
			Key[] keys = [new Key(SuiteHelpers.ns, SuiteHelpers.set, 989299024)];

			client.Delete(null, null, new SingleDeleteArrayHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleDeleteNotFoundSequence()
		{
			Key[] keys = [new Key(SuiteHelpers.ns, SuiteHelpers.set, 989299025)];

			client.Delete(null, null, new SingleDeleteSequenceHandler(this), keys);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleReadGetSequence()
		{
			List<BatchRead> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			client.Get(null, new SingleReadGetSequenceHandler(this), records);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateGet()
		{
			client.Get(null, new SingleOperateGetArrayHandler(this), [seedKey], Operation.Get(BinName));
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleOperateGetSequence()
		{
			client.Get(null, new SingleOperateGetSequenceHandler(this), [seedKey], Operation.Get(BinName));
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleWriteSequence()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-single-batch-write-seq");
			List<BatchRecord> records =
			[
				new BatchWrite(key, [Operation.Put(new Bin(BinName, ValuePrefix + "-write-seq"))])
			];

			client.Operate(null, new SingleWriteSequenceHandler(this, key), records);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncBatchSingleDeleteSequence()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-single-batch-delete-seq");
			client.Put(null, key, new Bin(BinName, ValuePrefix + "-delete-seq"));

			List<BatchRecord> records = [new BatchDelete(key)];
			client.Operate(null, new SingleDeleteOperateSequenceHandler(this, key), records);
			WaitTillComplete();
		}

		private class SeedWriteHandler(AsyncMonitor monitor) : WriteListener
		{
			public void OnSuccess(Key key)
			{
				monitor.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				monitor.SetError(e);
				monitor.NotifyCompleted();
			}
		}

		private class SingleGetArrayHandler(TestAsyncSingleBatch parent) : RecordArrayListener
		{
			public void OnSuccess(Key[] keys, Record[] records)
			{
				if (!parent.AssertRecordFound(keys[0], records[0]))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertBinEqual(keys[0], records[0], BinName, ValuePrefix);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleGetSequenceHandler(TestAsyncSingleBatch parent) : RecordSequenceListener
		{
			private bool received;

			public void OnRecord(Key key, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					return;
				}

				parent.AssertBinEqual(key, record, BinName, ValuePrefix);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleGetHeaderArrayHandler(TestAsyncSingleBatch parent) : RecordArrayListener
		{
			public void OnSuccess(Key[] keys, Record[] records)
			{
				if (!parent.AssertRecordFound(keys[0], records[0]))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertGreaterThanZero(records[0].generation);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleGetHeaderSequenceHandler(TestAsyncSingleBatch parent) : RecordSequenceListener
		{
			private bool received;

			public void OnRecord(Key key, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					return;
				}

				parent.AssertGreaterThanZero(record.generation);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleExistsArrayHandler(TestAsyncSingleBatch parent) : ExistsArrayListener
		{
			public void OnSuccess(Key[] keys, bool[] exists)
			{
				parent.AssertEquals(true, exists[0]);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleExistsSequenceHandler(TestAsyncSingleBatch parent) : ExistsSequenceListener
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

		private class SingleOperateListHandler(TestAsyncSingleBatch parent) : BatchOperateListListener
		{
			public void OnSuccess(List<BatchRecord> records, bool status)
			{
				BatchRead batchRead = (BatchRead)records[0];
				if (!parent.AssertRecordFound(batchRead.key, batchRead.record))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertBinEqual(batchRead.key, batchRead.record, BinName, ValuePrefix);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateSequenceHandler(TestAsyncSingleBatch parent) : BatchRecordSequenceListener
		{
			private bool received;

			public void OnRecord(BatchRecord record, int index)
			{
				BatchRead batchRead = (BatchRead)record;
				if (!parent.AssertRecordFound(batchRead.key, batchRead.record))
				{
					return;
				}

				parent.AssertBinEqual(batchRead.key, batchRead.record, BinName, ValuePrefix);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateWriteHandler(TestAsyncSingleBatch parent, Key key) : BatchOperateListListener
		{
			public void OnSuccess(List<BatchRecord> records, bool status)
			{
				client.Get(null, new VerifyWriteHandler(parent, key), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class VerifyWriteHandler(TestAsyncSingleBatch parent, Key key) : RecordListener
		{
			public void OnSuccess(Key readKey, Record record)
			{
				parent.AssertBinEqual(key, record, BinName, ValuePrefix + "-write");
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateKeysHandler(TestAsyncSingleBatch parent) : BatchRecordArrayListener
		{
			public void OnSuccess(BatchRecord[] records, bool status)
			{
				Record record = records[0].record;
				if (!parent.AssertRecordFound(seedKey, record))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertBinEqual(seedKey, record, BinName, ValuePrefix);
				parent.NotifyCompleted();
			}

			public void OnFailure(BatchRecord[] records, AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateKeysSequenceHandler(TestAsyncSingleBatch parent) : BatchRecordSequenceListener
		{
			private bool received;

			public void OnRecord(BatchRecord record, int index)
			{
				if (!parent.AssertRecordFound(seedKey, record.record))
				{
					return;
				}

				parent.AssertBinEqual(seedKey, record.record, BinName, ValuePrefix);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleDeleteArrayHandler(TestAsyncSingleBatch parent) : BatchRecordArrayListener
		{
			public void OnSuccess(BatchRecord[] records, bool status)
			{
				parent.AssertEquals(false, status);
				parent.AssertEquals(ResultCode.KEY_NOT_FOUND_ERROR, records[0].resultCode);
				parent.NotifyCompleted();
			}

			public void OnFailure(BatchRecord[] records, AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleDeleteSequenceHandler(TestAsyncSingleBatch parent) : BatchRecordSequenceListener
		{
			private bool received;

			public void OnRecord(BatchRecord record, int index)
			{
				parent.AssertEquals(ResultCode.KEY_NOT_FOUND_ERROR, record.resultCode);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleReadGetSequenceHandler(TestAsyncSingleBatch parent) : BatchSequenceListener
		{
			private bool received;

			public void OnRecord(BatchRead record)
			{
				if (!parent.AssertRecordFound(record.key, record.record))
				{
					return;
				}

				parent.AssertBinEqual(record.key, record.record, BinName, ValuePrefix);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateGetArrayHandler(TestAsyncSingleBatch parent) : RecordArrayListener
		{
			public void OnSuccess(Key[] keys, Record[] records)
			{
				if (!parent.AssertRecordFound(keys[0], records[0]))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertBinEqual(keys[0], records[0], BinName, ValuePrefix);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleOperateGetSequenceHandler(TestAsyncSingleBatch parent) : RecordSequenceListener
		{
			private bool received;

			public void OnRecord(Key key, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					return;
				}

				parent.AssertBinEqual(key, record, BinName, ValuePrefix);
				received = true;
			}

			public void OnSuccess()
			{
				if (!parent.AssertTrue(received))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleWriteSequenceHandler(TestAsyncSingleBatch parent, Key key) : BatchRecordSequenceListener
		{
			public void OnRecord(BatchRecord record, int index)
			{
			}

			public void OnSuccess()
			{
				client.Get(null, new VerifyWriteSeqHandler(parent, key), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class VerifyWriteSeqHandler(TestAsyncSingleBatch parent, Key key) : RecordListener
		{
			public void OnSuccess(Key readKey, Record record)
			{
				parent.AssertBinEqual(key, record, BinName, ValuePrefix + "-write-seq");
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class SingleDeleteOperateSequenceHandler(TestAsyncSingleBatch parent, Key key) : BatchRecordSequenceListener
		{
			public void OnRecord(BatchRecord record, int index)
			{
			}

			public void OnSuccess()
			{
				client.Exists(null, new VerifyDeleteSeqHandler(parent), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class VerifyDeleteSeqHandler(TestAsyncSingleBatch parent) : ExistsListener
		{
			public void OnSuccess(Key key, bool exists)
			{
				parent.AssertEquals(false, exists);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
