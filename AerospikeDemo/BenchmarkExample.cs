using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	abstract class BenchmarkExample : Example
	{
        protected BenchmarkArguments args;
        protected BenchmarkShared shared;
        private BenchmarkThread[] threads;
        private Thread tickerThread;

        public BenchmarkExample(Console console)
			: base(console)
		{
        }

        public override void RunExample(Arguments a)
        {
            this.args = (BenchmarkArguments)a;
            shared = new BenchmarkShared(args);

            if (args.sync)
            {
                ClientPolicy policy = new ClientPolicy();
                AerospikeClient client = new AerospikeClient(policy, args.host, args.port);

                try
                {
                    threads = new BenchmarkThreadSync[args.threadMax];
                    for (int i = 0; i < args.threadMax; i++)
                    {
                        threads[i] = new BenchmarkThreadSync(console, args, shared, this, client);
                    }
                    RunThreads();
                }
                finally
                {
                    client.Close();
                }
            }
            else
            {
                console.Info("Maximum concurrent commands: " + args.commandMax);

                AsyncClientPolicy policy = new AsyncClientPolicy();
                policy.asyncMaxCommands = args.commandMax;

                AsyncClient client = new AsyncClient(policy, args.host, args.port);

                try
                {
                    threads = new BenchmarkThreadAsync[args.threadMax];
                    for (int i = 0; i < args.threadMax; i++)
                    {
                        threads[i] = new BenchmarkThreadAsync(console, args, shared, this, client);
                    }
                    RunThreads();
                }
                finally
                {
                    client.Close();
                }
            }
        }
        
        private void RunThreads()
        {
            RunBegin();

            console.Info("Start " + threads.Length + " generator threads");
            valid = true;
            tickerThread = new Thread(new ThreadStart(this.Ticker));
            tickerThread.Start();

            foreach (BenchmarkThread thread in threads)
            {
                thread.Start();
            }

            foreach (BenchmarkThread thread in threads)
            {
                thread.Join();
            }

            valid = false;
            tickerThread.Join();
        }

		private void Ticker()
		{
			try
			{
				RunTicker();
			}
			catch (Exception ex)
			{
				console.Error(ex.Message);
			}
		}

        protected bool GetIsStopWrites()
		{
			string filter = "namespace/" + args.ns;
			string tokens;

			try
			{
                tokens = Info.Request(args.host, args.port, filter);
			}
			catch (Exception)
			{
				return true;
			}

			if (tokens == null)
			{
				return false;
			}

			string name = "stop-writes";
			string search = name + '=';
			int begin = tokens.IndexOf(search);

			if (begin < 0)
				return false;

			begin += search.Length;
			int end = tokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = tokens.Length;
			}
			string value = tokens.Substring(begin, end - begin);
			return Boolean.Parse(value);
		}
    
        protected abstract void RunBegin();
        protected abstract void RunTicker();
    }
}
