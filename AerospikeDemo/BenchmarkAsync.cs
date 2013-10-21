using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkAsync : AsyncExample
	{
		public BenchmarkAsync(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Benchmark asynchronous random write/read/delete performance.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			console.Info("Maximum concurrent commands: " + args.commandMax);
			BenchmarkAsynchronous benchmark = new BenchmarkAsynchronous(console, this, client);
			benchmark.RunExample(args, 1);
		}
	}

	class BenchmarkAsynchronous : Benchmark
	{
		private BenchmarkAsync example;
		private AsyncClient client;

		public BenchmarkAsynchronous(Console console, BenchmarkAsync example, AsyncClient client)
			: base(console)
		{
			this.example = example;
			this.client = client;
		}

		protected override void WriteRecord(int key, int value)
		{
			client.Put(policy, new WriteHandler(this, key, value), new Key(ns, setName, key), new Bin(binName, value));
		}

		protected override void ReadRecord(int key, int expected, bool deleted)
		{
			client.Get(policy, new ReadHandler(this, key, expected, deleted), new Key(ns, setName, key), binName);
		}

		protected override void DeleteRecord(int key)
		{
			client.Delete(policy, new DeleteHandler(this, key), new Key(ns, setName, key));
		}

		protected override bool IsValid()
		{
			return example.valid;
		}

		protected override void SetValid(bool valid)
		{
			example.valid = valid;
		}

		private class WriteHandler : WriteListener
		{
			BenchmarkAsynchronous parent;
			int key;
			int value;

			public WriteHandler(BenchmarkAsynchronous parent, int key, int value)
			{
				this.parent = parent;
				this.key = key;
				this.value = value;
			}

			public void OnSuccess(Key k)
			{
				parent.OnWriteSuccess(key, value);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnWriteFailure(key, value, e);
			}
		}

		private class ReadHandler : RecordListener
		{
			BenchmarkAsynchronous parent;
			int key;
			int expected;
			bool deleted;

			public ReadHandler(BenchmarkAsynchronous parent, int key, int expected, bool deleted)
			{
				this.parent = parent;
				this.key = key;
				this.expected = expected;
				this.deleted = deleted;
			}

			public void OnSuccess(Key k, Record record)
			{
				parent.OnReadSuccess(key, expected, deleted, record);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnReadFailure(key, e);
			}
		}

		private class DeleteHandler : DeleteListener
		{
			BenchmarkAsynchronous parent;
			int key;

			public DeleteHandler(BenchmarkAsynchronous parent, int key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key k, bool found)
			{
				parent.OnDeleteSuccess(key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnDeleteFailure(key, e);
			}
		}
	}
}
