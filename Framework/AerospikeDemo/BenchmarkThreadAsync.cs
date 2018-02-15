/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Diagnostics;

namespace Aerospike.Demo
{
	class BenchmarkThreadAsync : BenchmarkThread
	{
		private AsyncClient client;

        public BenchmarkThreadAsync
        (
            Console console,
            BenchmarkArguments args,
            BenchmarkShared shared,
            Example example,
            AsyncClient client
        ) : base(console, args, shared, example)
		{
			this.client = client;
		}

        protected override void WriteRecord(WritePolicy policy, Key key, Bin bin)
		{
			// If timeout occurred, yield thread to back off throttle.
			// Fail counters are reset every second.
			if (shared.writeTimeoutCount > 0)
			{
				Thread.Yield();
			}

            if (shared.writeLatency != null)
            {
                client.Put(policy, new LatencyWriteHandler(this, key, bin), key, bin);
            }
            else
            {
                client.Put(policy, new WriteHandler(this, key, bin), key, bin);
            }
		}

        protected override void ReadRecord(Policy policy, Key key, string binName)
		{
			// If timeout occurred, yield thread to back off throttle.
			// Fail counters are reset every second.
			if (shared.readTimeoutCount > 0)
			{
				Thread.Yield();
			}

            if (shared.readLatency != null)
            {
                client.Get(policy, new LatencyReadHandler(this, key), key, binName);
            }
            else
            {
                client.Get(policy, new ReadHandler(this, key), key, binName);
            }
		}

		protected override void BatchRead(BatchPolicy policy, Key[] keys, string binName)
		{
			if (shared.readTimeoutCount > 0)
			{
				Thread.Yield();
			}

			if (shared.readLatency != null)
			{
				client.Get(policy, new LatencyBatchHandler(this), keys, binName);
			}
			else
			{
				client.Get(policy, new BatchHandler(this), keys, binName);
			}
		}

		private class WriteHandler : WriteListener
		{
            BenchmarkThreadAsync parent;
            Key key;
            Bin bin;

            public WriteHandler(BenchmarkThreadAsync parent, Key key, Bin bin)
			{
				this.parent = parent;
                this.key = key;
                this.bin = bin;
			}

			public void OnSuccess(Key k)
			{
				parent.OnWriteSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnWriteFailure(key, bin, e);
			}
		}

        private class LatencyWriteHandler : WriteListener
        {
            BenchmarkThreadAsync parent;
            Key key;
            Bin bin;
            Stopwatch watch;

            public LatencyWriteHandler(BenchmarkThreadAsync parent, Key key, Bin bin)
            {
                this.parent = parent;
                this.key = key;
                this.bin = bin;
                this.watch = Stopwatch.StartNew();
            }

            public void OnSuccess(Key k)
            {
                double elapsed = watch.Elapsed.TotalMilliseconds;
                parent.OnWriteSuccess(elapsed);
            }

            public void OnFailure(AerospikeException e)
            {
                parent.OnWriteFailure(key, bin, e);
            }
        }
        
        private class ReadHandler : RecordListener
		{
            BenchmarkThreadAsync parent;
			Key key;

            public ReadHandler(BenchmarkThreadAsync parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key k, Record record)
			{
				parent.OnReadSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnReadFailure(key, e);
			}
		}
    
        private class LatencyReadHandler : RecordListener
        {
            BenchmarkThreadAsync parent;
            Key key;
            Stopwatch watch;

            public LatencyReadHandler(BenchmarkThreadAsync parent, Key key)
            {
                this.parent = parent;
                this.key = key;
                this.watch = Stopwatch.StartNew();
            }

            public void OnSuccess(Key k, Record record)
            {
                double elapsed = watch.Elapsed.TotalMilliseconds;
                parent.OnReadSuccess(elapsed);
            }

            public void OnFailure(AerospikeException e)
            {
                parent.OnReadFailure(key, e);
            }
        }

		private class BatchHandler : RecordArrayListener
		{
			BenchmarkThreadAsync parent;

			public BatchHandler(BenchmarkThreadAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				parent.OnBatchSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnBatchFailure(e);
			}
		}

		private class LatencyBatchHandler : RecordArrayListener
		{
			BenchmarkThreadAsync parent;
			Stopwatch watch;

			public LatencyBatchHandler(BenchmarkThreadAsync parent)
			{
				this.parent = parent;
				this.watch = Stopwatch.StartNew();
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				double elapsed = watch.Elapsed.TotalMilliseconds;
				parent.OnBatchSuccess(elapsed);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnBatchFailure(e);
			}
		}
	}
}
