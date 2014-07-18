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

namespace Aerospike.Demo
{
	abstract class BenchmarkExample : Example
	{
        protected BenchmarkArguments args;
        protected BenchmarkShared shared;
        private BenchmarkThread[] threads;
        private Thread tickerThread;
        private AerospikeClient client;

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
				policy.user = args.user;
				policy.password = args.password;
				policy.failIfNotConnected = true;
				client = new AerospikeClient(policy, args.host, args.port);

                try
                {
                    args.SetServerSpecific(client);
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
				policy.user = args.user;
				policy.password = args.password;
				policy.failIfNotConnected = true;
				policy.asyncMaxCommands = args.commandMax;

                AsyncClient client = new AsyncClient(policy, args.host, args.port);
                this.client = client;

                try
                {
                    args.SetServerSpecific(client);
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
                tokens = Info.Request(client.Nodes[0], filter);
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
