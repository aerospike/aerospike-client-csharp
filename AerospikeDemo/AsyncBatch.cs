using System;
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
						parent.taskSize = 5;
						parent.BatchExistsArray();
						parent.BatchExistsSequence();
						parent.BatchGetArray();
						parent.BatchGetSequence();
						parent.BatchGetHeaders();
					}
					catch (Exception e)
					{
						parent.console.Error("Batch failed: namespace={0} set={1} key={2} exception={3}", 
							key.ns, key.setName, key.userKey, e.Message);
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
			client.Exists(args.policy, new ExistsArrayHandler(this), sendKeys);
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
			client.Exists(args.policy, new ExistsSequenceHandler(this), sendKeys);
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
			client.Get(args.policy, new RecordArrayHandler(this), sendKeys);
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
			client.Get(args.policy, new RecordSequenceHandler(this), sendKeys);
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
			client.GetHeader(args.policy, new RecordHeaderArrayHandler(this), sendKeys);
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