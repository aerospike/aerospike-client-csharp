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
using System.Threading;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Demo
{
    class BenchmarkInitialize : BenchmarkExample
	{
		private StringBuilder latencyBuilder;
		private string latencyHeader;

        public BenchmarkInitialize(Console console)
			: base(console)
		{
        }

        protected override void RunBegin()
        {
            console.Info("Initialize " + args.recordsInit + " records");
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
			}
        }

		private void WriteTicker()
		{
			int writeCurrent = Interlocked.Exchange(ref shared.writeCount, 0);
			int writeTimeoutCurrent = Interlocked.Exchange(ref shared.writeTimeoutCount, 0);
			int writeErrorCurrent = Interlocked.Exchange(ref shared.writeErrorCount, 0);
			int totalCount = shared.currentKey;

			long elapsed = shared.periodBegin.ElapsedMilliseconds;
			shared.periodBegin.Restart();

			double writeTps = Math.Round((double)writeCurrent * 1000 / elapsed, 0);

			console.Info("write(tps={0} timeouts={1} errors={2} total={3})",
				writeTps, writeTimeoutCurrent, writeErrorCurrent, totalCount
			);

			if (shared.writeLatency != null)
			{
				if (latencyHeader != null)
				{
					console.Write(latencyHeader);
				}
				console.Write(shared.writeLatency.PrintResults(latencyBuilder, "write"));
			}
		}
    }
}
