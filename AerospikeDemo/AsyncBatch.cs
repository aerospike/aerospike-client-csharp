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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class AsyncBatch : AsyncExample
	{
		private const string KeyPrefix = "batchkey";
		private const string ValuePrefix = "batchvalue";
		private const int BatchSize = 8;

		private AsyncClient client;
		private Arguments args;
		private Key[] sendKeys;
		private string binName;
		private int taskCount;
		private int taskSize;
		private bool completed;

		public AsyncBatch(Console console) : base(console)
		{
		}

		/// <summary>
		/// Asynchronous batch examples.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			this.client = client;
			this.args = args;
			this.binName = args.GetBinName("batchbin");
			this.taskCount = 0;
			this.taskSize = 0;
			this.completed = false;

			InitializeKeys();
			WriteRecords();
			WaitTillComplete();
		}

		private void InitializeKeys()
		{
			sendKeys = new Key[BatchSize];

			for (int i = 0; i < BatchSize; i++)
			{
				sendKeys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}
		}

		/// <summary>
		/// Write records individually.
		/// </summary>
		private void WriteRecords()
		{
			WriteHandler handler = new WriteHandler(this, BatchSize);

			for (int i = 1; i <= BatchSize; i++)
			{
				Key key = sendKeys[i - 1];
				Bin bin = new Bin(binName, ValuePrefix + i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, handler, key, bin);
			}
		}

		private class WriteHandler : WriteListener
		{
			private readonly AsyncBatch parent;
			internal readonly int max;
			internal int count;

			public WriteHandler(AsyncBatch parent, int max)
			{
				this.parent = parent;
				this.max = max;
			}

			public virtual void OnSuccess(Key key)
			{
				int rows = Interlocked.Increment(ref count);

				if (rows == max)
				{
					try
					{
						// All writes succeeded. Run batch queries in parallel.
						parent.taskSize = 6;
						parent.BatchExistsArray();
						parent.BatchExistsSequence();
						parent.BatchGetArray();
						parent.BatchGetSequence();
						parent.BatchGetHeaders();
						parent.BatchReadComplex();
					}
					catch (Exception e)
					{
						parent.console.Error("Batch failed: " + e.Message);
						parent.AllTasksComplete();
					}
				}
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Put failed: " + e.Message);
				parent.AllTasksComplete();
			}
		}

		/// <summary>
		/// Check existence of records in one batch, receive in one array.
		/// </summary>
		private void BatchExistsArray()
		{
			client.Exists(null, new ExistsArrayHandler(this), sendKeys);
		}

		private class ExistsArrayHandler : ExistsArrayListener
		{
			private readonly AsyncBatch parent;

			public ExistsArrayHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnSuccess(Key[] keys, bool[] existsArray)
			{
				for (int i = 0; i < existsArray.Length; i++)
				{
					Key key = keys[i];
					bool exists = existsArray[i];
					parent.console.Info("Record: namespace={0} set={1} key={2} exists={3}", 
						key.ns, key.setName, key.userKey, exists);
				}
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch exists array failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}

		/// <summary>
		/// Check existence of records in one batch, receive one record at a time.
		/// </summary>
		private void BatchExistsSequence()
		{
			client.Exists(null, new ExistsSequenceHandler(this), sendKeys);
		}

		private class ExistsSequenceHandler : ExistsSequenceListener
		{
			private readonly AsyncBatch parent;

			public ExistsSequenceHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnExists(Key key, bool exists)
			{
				parent.console.Info("Record: namespace={0} set={1} key={2} exists={3}", 
					key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), exists);
			}

			public virtual void OnSuccess()
			{
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch exists sequence failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}

		/// <summary>
		/// Read records in one batch, receive in array.
		/// </summary>
		private void BatchGetArray()
		{
			client.Get(null, new RecordArrayHandler(this), sendKeys);
		}

		private class RecordArrayHandler : RecordArrayListener
		{
			private readonly AsyncBatch parent;

			public RecordArrayHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnSuccess(Key[] keys, Record[] records)
			{
				for (int i = 0; i < records.Length; i++)
				{
					Key key = keys[i];
					Record record = records[i];
					Log.Level level = Log.Level.ERROR;
					object value = null;

					if (record != null)
					{
						level = Log.Level.INFO;
						value = record.GetValue(parent.binName);
					}
					parent.console.Write(level, "Record: namespace={0} set={1} key={2} bin={3} value={4}", 
						key.ns, key.setName, key.userKey, parent.binName, value);
				}

				if (records.Length != BatchSize)
				{
					parent.console.Error("Record size mismatch. Expected {0}. Received {1}.", 
						BatchSize, records.Length);
				}
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch get array failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}

		/// <summary>
		/// Read records in one batch call, receive one record at a time.
		/// </summary>
		private void BatchGetSequence()
		{
			client.Get(null, new RecordSequenceHandler(this), sendKeys);
		}

		private class RecordSequenceHandler : RecordSequenceListener
		{
			private readonly AsyncBatch parent;

			public RecordSequenceHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnRecord(Key key, Record record)
			{
				Log.Level level = Log.Level.ERROR;
				object value = null;

				if (record != null)
				{
					level = Log.Level.INFO;
					value = record.GetValue(parent.binName);
				}
				parent.console.Write(level, "Record: namespace={0} set={1} key={2} bin={3} value={4}",
					key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), parent.binName, value);
			}

			public virtual void OnSuccess()
			{
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch get sequence failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}

		/// <summary>
		/// Read record headers in one batch, receive in an array.
		/// </summary>
		private void BatchGetHeaders()
		{
			client.GetHeader(null, new RecordHeaderArrayHandler(this), sendKeys);
		}

		private class RecordHeaderArrayHandler: RecordArrayListener
		{
			private readonly AsyncBatch parent;

			public RecordHeaderArrayHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnSuccess(Key[] keys, Record[] records)
			{
				for (int i = 0; i < records.Length; i++)
				{
					Key key = keys[i];
					Record record = records[i];
					Log.Level level = Log.Level.ERROR;
					int generation = 0;
					int expiration = 0;

					if (record != null && (record.generation > 0 || record.expiration > 0))
					{
						level = Log.Level.INFO;
						generation = record.generation;
						expiration = record.expiration;
					}
					parent.console.Write(level, "Record: namespace={0} set={1} key={2} generation={3} expiration={4}", 
						key.ns, key.setName, key.userKey, generation, expiration);
				}

				if (records.Length != BatchSize)
				{
					parent.console.Error("Record size mismatch. Expected {0}. Received {1}.", BatchSize, records.Length);
				}
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch get headers failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}

		private void BatchReadComplex()
		{
			// Batch gets into one call.
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
			string[] bins = new string[] { binName };
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 6), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 8), new string[] { "binnotfound" }));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			client.Get(null, new BatchListHandler(this), records);
		}

		private class BatchListHandler : BatchListListener
		{
			private readonly AsyncBatch parent;

			public BatchListHandler(AsyncBatch parent)
			{
				this.parent = parent;
			}

			public virtual void OnSuccess(List<BatchRead> records)
			{
				// Show results.
				int found = 0;
				foreach (BatchRead record in records)
				{
					Key key = record.key;
					Record rec = record.record;

					if (rec != null)
					{
						found++;
						parent.console.Info("Record: ns={0} set={1} key={2} bin={3} value={4}",
							key.ns, key.setName, key.userKey, parent.binName, rec.GetValue(parent.binName));
					}
					else
					{
						parent.console.Info("Record not found: ns={0} set={1} key={2} bin={3}",
							key.ns, key.setName, key.userKey, parent.binName);
					}
				}

				if (found != 8)
				{
					parent.console.Error("Records found mismatch. Expected 8. Received " + found);
				}
				parent.TaskComplete();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Batch read complex failed: " + Util.GetErrorMessage(e));
				parent.TaskComplete();
			}
		}
		
		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void TaskComplete()
		{
			if (Interlocked.Increment(ref taskCount) >= taskSize)
			{
				AllTasksComplete();
			}
		}

		private void AllTasksComplete()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}
}
