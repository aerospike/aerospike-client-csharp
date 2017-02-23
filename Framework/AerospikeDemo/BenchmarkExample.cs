/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
				policy.tlsPolicy = args.tlsPolicy;
				policy.requestProleReplicas = args.requestProleReplicas;
				client = new AerospikeClient(policy, args.hosts);

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
				policy.tlsPolicy = args.tlsPolicy;
				policy.requestProleReplicas = args.requestProleReplicas;
				policy.asyncMaxCommands = args.commandMax;

                AsyncClient client = new AsyncClient(policy, args.hosts);
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
			string tokens = null;

			Node[] nodes = client.Nodes;

			foreach (Node node in nodes)
			{
				try
				{
					tokens = Info.Request(node, filter);

					if (tokens != null)
					{
						break;
					}
				}
				catch (Exception)
				{
					// Try next node.
				}
			}

			if (tokens == null)
			{
				// None of the nodes responded.  Shutdown.
				return true;
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
