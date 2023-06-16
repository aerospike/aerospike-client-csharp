/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Text;
using Aerospike.Client;

namespace Aerospike.Benchmarks
{
	sealed class ReadWrite
	{
		private readonly Args args;
		private readonly Metrics metrics;
		private readonly ILatencyManager latencyManager;

		public ReadWrite(Args args, Metrics metrics, ILatencyManager latencyManager)
		{
			this.args = args;
			this.metrics = metrics;
			this.latencyManager = latencyManager;
		}

		public void RunSync(AerospikeClient client)
		{
			ReadWriteTaskSync[] tasks = new ReadWriteTaskSync[args.threadMax];

			for (long i = 0; i < args.threadMax; i++)
			{
				tasks[i] = new ReadWriteTaskSync(client, args, metrics, latencyManager);
			}

			var ticker = new Ticker(args, metrics, latencyManager);
			ticker.Run();

			foreach (ReadWriteTaskSync task in tasks)
			{
				task.Start();
			}

			ticker.WaitForAllToPrint();
			//ticker.Stop();
		}

		public void RunAsync(AsyncClient client)
		{
			// Generate commandMax writes to seed the event loops.
			// Then start a new command in each command callback.
			// This effectively throttles new command generation, by only allowing
			// commandMax at any point in time.
			int maxConcurrentCommands = args.commandMax;

			if (maxConcurrentCommands > args.recordsInit)
			{
				maxConcurrentCommands = args.recordsInit;
			}

			ReadWriteTaskAsync[] tasks = new ReadWriteTaskAsync[maxConcurrentCommands];

			for (int i = 0; i < maxConcurrentCommands; i++)
			{
				tasks[i] = new ReadWriteTaskAsync(client, args, metrics, latencyManager);
			}

			var ticker = new Ticker(args, metrics, latencyManager);
			ticker.Run();

			foreach (ReadWriteTaskAsync task in tasks)
			{
				task.Start();
			}
			ticker.WaitForAllToPrint();
			//ticker.Stop();
		}

		/*private void RunTicker(ReadWriteTask[] tasks)
		{
			StringBuilder latencyBuilder = null;
			string latencyHeader = null;

			if (metrics.writeLatency != null)
			{
				latencyBuilder = new StringBuilder(200);
				latencyHeader = metrics.writeLatency.PrintHeader();
			}

			// Give tasks a chance to create stats for first period.
			Thread.Sleep(900);

			long transactionTotal = 0;

			while (true)
			{
				long time = metrics.Time;

				int writeCurrent = Interlocked.Exchange(ref metrics.writeCount, 0);
				int writeTimeoutCurrent = Interlocked.Exchange(ref metrics.writeTimeoutCount, 0);
				int writeErrorCurrent = Interlocked.Exchange(ref metrics.writeErrorCount, 0);
				int readCurrent = Interlocked.Exchange(ref metrics.readCount, 0);
				int readTimeoutCurrent = Interlocked.Exchange(ref metrics.readTimeoutCount, 0);
				int readErrorCurrent = Interlocked.Exchange(ref metrics.readErrorCount, 0);

				long elapsed = metrics.NextPeriod(time);
				long writeTps = (long)writeCurrent * 1000L / elapsed;
				long readTps = (long)readCurrent * 1000L / elapsed;
				string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

				Console.WriteLine(dt + " write(tps={0} timeouts={1} errors={2}) read(tps={3} timeouts={4} errors={5}) total(tps={6} timeouts={7} errors={8})",
					writeTps, writeTimeoutCurrent, writeErrorCurrent,
					readTps, readTimeoutCurrent, readErrorCurrent,
					writeTps + readTps, writeTimeoutCurrent + readTimeoutCurrent, writeErrorCurrent + readErrorCurrent);

				if (metrics.writeLatency != null)
				{
					if (latencyHeader != null)
					{
						Console.WriteLine(latencyHeader);
					}
					Console.WriteLine(metrics.writeLatency.PrintResults(latencyBuilder, "write"));
					Console.WriteLine(metrics.readLatency.PrintResults(latencyBuilder, "read"));
				}

				if (args.transactionMax > 0)
				{
					transactionTotal += writeCurrent + writeTimeoutCurrent + writeErrorCurrent +
						readCurrent + readTimeoutCurrent + readErrorCurrent;

					if (transactionTotal >= args.transactionMax)
					{
						foreach (ReadWriteTask task in tasks)
						{
							task.Stop();
						}

						if (metrics.writeLatency != null)
						{
							Console.WriteLine("Latency Summary");

							if (latencyHeader != null)
							{
								Console.WriteLine(latencyHeader);
							}
							Console.WriteLine(metrics.writeLatency.PrintSummary(latencyBuilder, "write"));
							Console.WriteLine(metrics.readLatency.PrintSummary(latencyBuilder, "read"));
						}

						Console.WriteLine("Transaction limit reached: " + args.transactionMax + ". Exiting.");
						break;
					}
				}
				Thread.Sleep(1000);
			}
		}*/
	}
}
