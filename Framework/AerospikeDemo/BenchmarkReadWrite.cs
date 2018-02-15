/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Text;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkReadWrite : BenchmarkExample
	{
		private StringBuilder latencyBuilder;
		private string latencyHeader;
		
		public BenchmarkReadWrite(Console console)
			: base(console)
		{
        }

        protected override void RunBegin()
        {
            console.Info("Read/write using " + args.records + " records");
            args.recordsInit = 0;
        }

        protected override void RunTicker()
		{
            if (shared.writeLatency != null)
            {
                latencyBuilder = new StringBuilder(200);
                latencyHeader = shared.writeLatency.PrintHeader();
            }

			shared.periodBegin.Start();

			while (valid)
			{
				Thread.Sleep(1000);
				WriteTicker();
			}
			WriteTicker();

			if (shared.writeLatency != null)
			{
				console.Write("Latency Summary");

				if (latencyHeader != null)
				{
					console.Write(latencyHeader);
				}
				console.Write(shared.writeLatency.PrintSummary(latencyBuilder, "write"));
				console.Write(shared.readLatency.PrintSummary(latencyBuilder, "read"));
			}
		}

		private void WriteTicker()
		{
			int writeCurrent = Interlocked.Exchange(ref shared.writeCount, 0);
			int writeTimeoutCurrent = Interlocked.Exchange(ref shared.writeTimeoutCount, 0);
			int writeErrorCurrent = Interlocked.Exchange(ref shared.writeErrorCount, 0);
			int readCurrent = Interlocked.Exchange(ref shared.readCount, 0);
			int readTimeoutCurrent = Interlocked.Exchange(ref shared.readTimeoutCount, 0);
			int readErrorCurrent = Interlocked.Exchange(ref shared.readErrorCount, 0);

			long elapsed = shared.periodBegin.ElapsedMilliseconds;
			shared.periodBegin.Restart();

			double writeTps = Math.Round((double)writeCurrent * 1000 / elapsed, 0);
			double readTps = Math.Round((double)readCurrent * 1000 / elapsed, 0);

			console.Info("write(tps={0} timeouts={1} errors={2}) read(tps={3} timeouts={4} errors={5}) total(tps={6} timeouts={7} errors={8})",
				writeTps, writeTimeoutCurrent, writeErrorCurrent,
				readTps, readTimeoutCurrent, readErrorCurrent,
				writeTps + readTps, writeTimeoutCurrent + readTimeoutCurrent, writeErrorCurrent + readErrorCurrent);

			if (shared.writeLatency != null)
			{
				if (latencyHeader != null)
				{
					console.Write(latencyHeader);
				}
				console.Write(shared.writeLatency.PrintResults(latencyBuilder, "write"));
				console.Write(shared.readLatency.PrintResults(latencyBuilder, "read"));
			}

			/*
			int minw, minp, maxw, maxp, aw, ap;
			ThreadPool.GetMinThreads(out minw, out minp);
			ThreadPool.GetMaxThreads(out maxw, out maxp);
			ThreadPool.GetAvailableThreads(out aw, out ap);
			int t = Process.GetCurrentProcess().Threads.Count;
			console.Info("threads=" + t + ",minw=" + minw + ",minp=" + minp + ",maxw=" + maxw + ",maxp=" + maxp + ",aw=" + aw + ",ap=" + ap);
			*/
		}
    }
}
