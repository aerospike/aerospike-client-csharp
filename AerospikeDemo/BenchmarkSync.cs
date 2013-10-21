using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkSync : SyncExample
	{
		public BenchmarkSync(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Benchmark synchronous random write/read/delete performance.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			BenchmarkSynchronous benchmark = new BenchmarkSynchronous(console, this, client);
			benchmark.RunExample(args, args.threadMax);
		}
	}

	class BenchmarkSynchronous : Benchmark
	{
		private BenchmarkSync example;
		private AerospikeClient client;

		public BenchmarkSynchronous(Console console, BenchmarkSync example, AerospikeClient client)
			: base(console)
		{
			this.example = example;
			this.client = client;
		}

		protected override void WriteRecord(int key, int value)
		{
			client.Put(policy, new Key(ns, setName, key), new Bin(binName, value));
			OnWriteSuccess(key, value);
		}

		protected override void ReadRecord(int key, int expected, bool deleted)
		{
			Record record = client.Get(policy, new Key(ns, setName, key), binName);
			OnReadSuccess(key, expected, deleted, record);
		}

		protected override void DeleteRecord(int key)
		{
			client.Delete(policy, new Key(ns, setName, key));
			OnDeleteSuccess(key);
		}

		protected override bool IsValid()
		{
			return example.valid;
		}

		protected override void SetValid(bool valid)
		{
			example.valid = valid;
		}
	}
}
