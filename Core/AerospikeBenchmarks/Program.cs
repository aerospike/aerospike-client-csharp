/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Benchmarks
{
	class Program
	{
		static void Main(string[] args)
		{
			try
			{
				RunBenchmarks();
			}
			catch (Exception e)
			{
				Console.WriteLine("Error: " + e.Message);
				Console.WriteLine(e.StackTrace);
			}
		}

		private static void RunBenchmarks()
		{
			Log.SetLevel(Log.Level.INFO);
			Log.SetCallback(LogCallback);

			Args args = new Args();
			args.Print();

			Metrics metrics = new Metrics(args);

			if (args.sync)
			{
				ClientPolicy policy = new ClientPolicy();
				policy.user = args.user;
				policy.password = args.password;
				policy.tlsPolicy = args.tlsPolicy;
				policy.authMode = args.authMode;
				AerospikeClient client = new AerospikeClient(policy, args.hosts);

				try
				{
					args.SetServerSpecific(client);

					if (args.initialize)
					{
						Initialize prog = new Initialize(args, metrics);
						prog.RunSync(client);
					}
					else
					{
						ReadWrite prog = new ReadWrite(args, metrics);
						prog.RunSync(client);
					}
				}
				finally
				{
					client.Close();
				}
			}
			else
			{
				AsyncClientPolicy policy = new AsyncClientPolicy();
				policy.user = args.user;
				policy.password = args.password;
				policy.tlsPolicy = args.tlsPolicy;
				policy.authMode = args.authMode;
				policy.asyncMaxCommands = args.commandMax;

				AsyncClient client = new AsyncClient(policy, args.hosts);

				try
				{
					args.SetServerSpecific(client);

					if (args.initialize)
					{
						Initialize prog = new Initialize(args, metrics);
						prog.RunAsync(client);
					}
					else
					{
						ReadWrite prog = new ReadWrite(args, metrics);
						prog.RunAsync(client);
					}
				}
				finally
				{
					client.Close();
				}
			}
		}

		private static void LogCallback(Log.Level level, string message)
		{
			Console.WriteLine(level.ToString() + ' ' + message);
		}
	}
}
