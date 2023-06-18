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
	sealed class Initialize
	{
		private readonly Args args;
		private readonly Metrics metrics;

		public Initialize(Args args, Metrics metrics)
		{
			this.args = args;
			this.metrics = metrics;
		}

		public void RunSync(AerospikeClient client)
		{
			long taskCount = args.threadMax < args.recordsInit ? args.threadMax : args.recordsInit;
			long keysPerTask = args.recordsInit / taskCount;
			long rem = args.recordsInit - (keysPerTask * taskCount);
			long keyStart = 0;

			WriteTaskSync[] tasks = new WriteTaskSync[taskCount];

			for (long i = 0; i < taskCount; i++)
			{
				long keyCount = (i < rem) ? keysPerTask + 1 : keysPerTask;
				tasks[i] = new WriteTaskSync(client, args, metrics, keyStart, keyCount);
				keyStart += keyCount;
			}

			metrics.Start();

			foreach (WriteTaskSync task in tasks)
			{
				task.Start();
			}
			RunTicker();
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

			long keysPerCommand = args.recordsInit / maxConcurrentCommands;
			long keysRem = args.recordsInit - (keysPerCommand * maxConcurrentCommands);
			long keyStart = 0;

			WriteTaskAsync[] tasks = new WriteTaskAsync[maxConcurrentCommands];

			for (int i = 0; i < maxConcurrentCommands; i++)
			{
				// Allocate separate tasks for each seed command and reuse them in callbacks.
				long keyCount = (i < keysRem) ? keysPerCommand + 1 : keysPerCommand;
				tasks[i] = new WriteTaskAsync(client, args, metrics, keyStart, keyCount);
				keyStart += keyCount;
			}

			metrics.Start();

			foreach (WriteTaskAsync task in tasks)
			{
				task.Start();
			}
			RunTicker();
		}

		private void RunTicker()
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

			long total = 0;

			while (total < args.recordsInit)
			{
				long time = metrics.Time;

				int writeCurrent = Interlocked.Exchange(ref metrics.writeCount, 0);
				int writeTimeoutCurrent = Interlocked.Exchange(ref metrics.writeTimeoutCount, 0);
				int writeErrorCurrent = Interlocked.Exchange(ref metrics.writeErrorCount, 0);
				total += writeCurrent;

				long elapsed = metrics.NextPeriod(time);
				var writeTps = writeCurrent / TimeSpan.FromMilliseconds( (double) elapsed).TotalSeconds;
				string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

				Console.WriteLine(dt + " write(count={0} tps={1:########0} timeouts={2} errors={3})",
					total, writeTps, writeTimeoutCurrent, writeErrorCurrent);

				if (metrics.writeLatency != null)
				{
					if (latencyHeader != null)
					{
						Console.WriteLine(latencyHeader);
					}
					Console.WriteLine(metrics.writeLatency.PrintResults(latencyBuilder, "write"));
				}
				Thread.Sleep(1000);
			}

			if (metrics.writeLatency != null)
			{
				Console.WriteLine("Latency Summary");

				if (latencyHeader != null)
				{
					Console.WriteLine(latencyHeader);
				}
				Console.WriteLine(metrics.writeLatency.PrintSummary(latencyBuilder, "write"));
			}
		}
	}
}
