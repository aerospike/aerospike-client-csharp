using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	abstract class BenchmarkThread
	{
        private static int seed = Environment.TickCount;

		protected readonly Console console;
        protected readonly BenchmarkArguments args;
        protected readonly BenchmarkShared shared;
        private readonly Example example;
        private readonly Random random;
        private Thread thread;

        public BenchmarkThread(Console console, BenchmarkArguments args, BenchmarkShared shared, Example example)
		{
			this.console = console;
            this.args = args;
            this.shared = shared;
            this.example = example;
            random = new Random(Interlocked.Increment(ref seed));
		}

        public void Start()
        {
            thread = new Thread(new ThreadStart(this.Run));
            thread.Start();
        }

        public void Run()
        {
            try
            {
                if (args.recordsInit > 0)
                {
                    InitRecords();
                }
                else
                {
                    RunWorker();
                }
            }
            catch (Exception ex)
            {
                console.Error(ex.Message);
            }
        }

        public void Join()
        {
            thread.Join();
            thread = null;
        }

        private void InitRecords()
        {
            while (example.valid)
            {
                int key = Interlocked.Increment(ref shared.currentKey);

                if (key >= args.recordsInit)
                {
                    if (key == args.recordsInit)
                    {
                        console.Info("write(tps={0} fail={1} total={2}))",
                            shared.writeCount, shared.writeFailCount, args.recordsInit
                        );
                    }
                    break;
                }
                Write(key);
            }
        }
        
		private void RunWorker()
		{
            while (example.valid)
            {
                // Choose key at random.
                int key = random.Next(0, args.records);

                // Roll a percentage die.
                int die = random.Next(0, 100);

                if (die < args.readPct)
                {
                    Read(key);
                }
                else
                {
                    Write(key);
                }
            }
		}

		private void Write(int userKey)
		{
            Key key = new Key(args.ns, args.set, userKey);
            Bin bin = new Bin(args.binName, args.GetValue(random));

			try
			{
                WriteRecord(args.writePolicy, key, bin);
			}
			catch (Exception e)
			{
				OnWriteFailure(key, bin, e);
			}
		}

        private void Read(int userKey)
		{
            Key key = new Key(args.ns, args.set, userKey);
            
            try
			{
                ReadRecord(args.writePolicy, key, args.binName);
			}
			catch (Exception e)
			{
				OnReadFailure(key, e);
			}
		}

		protected void OnWriteSuccess()
		{
			Interlocked.Increment(ref shared.writeCount);
		}

        protected void OnWriteSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.writeCount);
            shared.writeLatency.Add(elapsed);
        }
        
        protected void OnWriteFailure(Key key, Bin bin, Exception e)
		{
			Interlocked.Increment(ref shared.writeFailCount);
			
            if (args.debug)
			{
				console.Error("Write error: ns={0} set={1} key={2} bin={3} value={4} exception={5}",
                    key.ns, key.setName, key.userKey, bin.name, bin.value, e.Message);
			}
	    }

		protected void OnReadSuccess()
		{
            Interlocked.Increment(ref shared.readCount);
		}

        protected void OnReadSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.readCount);
            shared.readLatency.Add(elapsed);
        }

        protected void OnReadFailure(Key key, Exception e)
		{
			OnReadFailure(key, e.Message);
		}

		protected void OnReadFailure(Key key, string message)
		{
			Interlocked.Increment(ref shared.readFailCount);

            if (args.debug)
			{
				console.Error("Read error: ns={0} set={1} key={2} exception={3}",
                    key.ns, key.setName, key.userKey, message);
			}
		}

		protected abstract void WriteRecord(WritePolicy policy, Key key, Bin bin);
		protected abstract void ReadRecord(Policy policy, Key key, string binName);
	}
}
