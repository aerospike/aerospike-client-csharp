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
using System.Diagnostics;

namespace Aerospike.Benchmarks
{
	sealed class ReadWriteTaskAsync : ReadWriteTask
	{
		private readonly AsyncClient client;
		private readonly RandomShift random;
		private readonly WriteListener writeListener;
		private readonly RecordListener recordListener;
		private readonly RecordArrayListener recordArrayListener;
		private readonly Stopwatch watch;
		private long begin;
		private readonly bool useLatency;

		public ReadWriteTaskAsync(AsyncClient client, Args args, Metrics metrics)
			: base(args, metrics)
		{
			this.client = client;
			this.random = new RandomShift();
			this.useLatency = metrics.writeLatency != null;

			if (useLatency)
			{
				writeListener = new LatencyWriteHandler(this);
				recordListener = new LatencyReadHandler(this);
				recordArrayListener = new LatencyBatchHandler(this);
			}
			else
			{
				writeListener = new WriteHandler(this);
				recordListener = new ReadHandler(this);
				recordArrayListener = new BatchHandler(this);
			}
			watch = Stopwatch.StartNew();
		}

		public void Start()
		{
			if (valid)
			{
				RunCommand();
			}
		}

		public void RunCommand()
		{
			// Roll a percentage die.
			int die = random.Next(0, 100);

			if (die < args.readPct)
			{
				if (args.batchSize <= 1)
				{
					int key = random.Next(0, args.records);
					Read(key);
				}
				else
				{
					BatchRead();
				}
			}
			else
			{
				// Perform Single record write even if in batch mode.
				int key = random.Next(0, args.records);
				Write(key);
			}
		}

		private void Write(int userKey)
		{
			Key key = new Key(args.ns, args.set, userKey);
			Bin bin = new Bin(args.binName, args.GetValue(random));

			try
			{
				if (useLatency)
				{
					begin = watch.ElapsedMilliseconds;
				}
				client.Put(args.writePolicy, writeListener, key, bin);
			}
			catch (AerospikeException ae)
			{
				WriteFailure(ae);
			}
			catch (Exception e)
			{
				WriteFailure(e);
			}
		}

		private class LatencyWriteHandler : WriteListener
		{
			private readonly ReadWriteTaskAsync parent;

			public LatencyWriteHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key)
			{
				parent.WriteSuccessLatency();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.WriteFailure(ae);
			}
		}

		private class WriteHandler : WriteListener
		{
			private readonly ReadWriteTaskAsync parent;

			public WriteHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key k)
			{
				parent.WriteSuccess();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.WriteFailure(ae);
			}
		}

		private void WriteSuccessLatency()
		{
			long elapsed = watch.ElapsedMilliseconds - Volatile.Read(ref begin);
			metrics.writeLatency.Add(elapsed);
			WriteSuccess();
		}

		private void WriteSuccess()
		{
			metrics.WriteSuccess();
			RunCommand();
		}

		private void WriteFailure(AerospikeException ae)
		{
			metrics.WriteFailure(ae);
			RunCommand();
		}

		private void WriteFailure(Exception e)
		{
			metrics.WriteFailure(e);
			RunCommand();
		}

		private void Read(int userKey)
		{
			Key key = new Key(args.ns, args.set, userKey);

			try
			{
				if (useLatency)
				{
					begin = watch.ElapsedMilliseconds;
				}
				client.Get(args.policy, recordListener, key, args.binName);
			}
			catch (AerospikeException ae)
			{
				ReadFailure(ae);
			}
			catch (Exception e)
			{
				ReadFailure(e);
			}
		}

		private class LatencyReadHandler : RecordListener
		{
			private readonly ReadWriteTaskAsync parent;

			public LatencyReadHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key k, Record record)
			{
				parent.ReadSuccessLatency();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.ReadFailure(ae);
			}
		}

		private class ReadHandler : RecordListener
		{
			private readonly ReadWriteTaskAsync parent;

			public ReadHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key k, Record record)
			{
				parent.ReadSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.ReadFailure(e);
			}
		}

		private void ReadSuccessLatency()
		{
			long elapsed = watch.ElapsedMilliseconds - Volatile.Read(ref begin);
			metrics.readLatency.Add(elapsed);
			ReadSuccess();
		}

		private void ReadSuccess()
		{
			metrics.ReadSuccess();
			RunCommand();
		}

		private void ReadFailure(AerospikeException ae)
		{
			metrics.ReadFailure(ae);
			RunCommand();
		}

		private void ReadFailure(Exception e)
		{
			metrics.ReadFailure(e);
			RunCommand();
		}

		private void BatchRead()
		{
			Key[] keys = new Key[args.batchSize];

			for (int i = 0; i < keys.Length; i++)
			{
				long keyIdx = random.Next(0, args.records);
				keys[i] = new Key(args.ns, args.set, keyIdx);
			}

			try
			{
				if (useLatency)
				{
					begin = watch.ElapsedMilliseconds;
				}
				client.Get(args.batchPolicy, recordArrayListener, keys, args.binName);
			}
			catch (AerospikeException ae)
			{
				BatchFailure(ae);
			}
			catch (Exception e)
			{
				BatchFailure(e);
			}
		}

		private class LatencyBatchHandler : RecordArrayListener
		{
			private readonly ReadWriteTaskAsync parent;

			public LatencyBatchHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				parent.BatchSuccessLatency();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.BatchFailure(ae);
			}
		}

		private class BatchHandler : RecordArrayListener
		{
			private readonly ReadWriteTaskAsync parent;

			public BatchHandler(ReadWriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				parent.BatchSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.BatchFailure(e);
			}
		}

		private void BatchSuccessLatency()
		{
			long elapsed = watch.ElapsedMilliseconds - Volatile.Read(ref begin);
			metrics.readLatency.Add(elapsed);
			ReadSuccess();
		}

		private void BatchSuccess()
		{
			metrics.ReadSuccess();
			RunCommand();
		}

		private void BatchFailure(AerospikeException ae)
		{
			metrics.ReadFailure(ae);
			RunCommand();
		}

		private void BatchFailure(Exception e)
		{
			metrics.ReadFailure(e);
			RunCommand();
		}
	}
}