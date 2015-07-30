/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncBatch : TestAsync
	{
		private const string keyPrefix = "asyncbatchkey";
		private const string valuePrefix = "batchvalue";
		private static readonly string binName = args.GetBinName("batchbin");
		private const int size = 8;
		private static Key[] sendKeys;

		[ClassInitialize()]
		public static void WriteRecords(TestContext testContext)
		{
			sendKeys = new Key[size];

			for (int i = 0; i < size; i++)
			{
				sendKeys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			AsyncMonitor monitor = new AsyncMonitor();
			WriteHandler handler = new WriteHandler(monitor, size);

			for (int i = 1; i <= size; i++)
			{
				Key key = sendKeys[i - 1];
				Bin bin = new Bin(binName, valuePrefix + i);
				client.Put(null, handler, key, bin);
			}
			monitor.WaitTillComplete();
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
				if (parent.AssertEquals(size, records.Length))
				{
					for (int i = 0; i < records.Length; i++)
					{
						if (!parent.AssertBinEqual(keys[i], records[i], binName, valuePrefix + (i + 1)))
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
					Object value = record.GetValue(binName);
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
				if (parent.AssertEquals(size, records.Length))
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
			string[] bins = new string[] { binName };
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 6), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 8), new string[] { "binnotfound" }));

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
						
						Object value = rec.GetValue(binName);
					
						if (count != 4 && count <= 7) {
							if (!parent.AssertEquals(valuePrefix + count, value))
							{
								parent.NotifyCompleted();
								return;
							}
						}
						else {
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
	}
}
