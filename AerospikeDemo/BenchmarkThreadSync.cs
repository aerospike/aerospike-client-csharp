using System;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkThreadSync : BenchmarkThread
	{
		private AerospikeClient client;

        public BenchmarkThreadSync
        (
            Console console,
            BenchmarkArguments args,
            BenchmarkShared shared,
            Example example,
            AerospikeClient client
        ) : base(console, args, shared, example)
		{
			this.client = client;
		}

		protected override void WriteRecord(WritePolicy policy, Key key, Bin bin)
		{
            if (shared.writeLatency != null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                client.Put(policy, key, bin);
                double elapsed = watch.Elapsed.TotalMilliseconds;
                OnWriteSuccess(elapsed);
            }
            else
            {
                client.Put(policy, key, bin);
                OnWriteSuccess();
            }
		}

		protected override void ReadRecord(Policy policy, Key key, string binName)
		{
            if (shared.readLatency != null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                Record record = client.Get(policy, key, binName);
                double elapsed = watch.Elapsed.TotalMilliseconds;
                OnReadSuccess(elapsed);
            }
            else
            {
                Record record = client.Get(policy, key, binName);
                OnReadSuccess();
            }           
		}
	}
}
