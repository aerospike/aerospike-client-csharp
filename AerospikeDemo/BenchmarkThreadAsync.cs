/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
    }
}
