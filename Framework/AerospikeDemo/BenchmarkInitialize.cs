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
            DateTime prevTime = DateTime.Now;
            StringBuilder latencyBuilder = null;
            string latencyHeader = null;

            if (shared.writeLatency != null)
            {
                latencyBuilder = new StringBuilder(200);
                latencyHeader = shared.writeLatency.PrintHeader();
            }

			while (valid)
			{
				int writeCurrent = Interlocked.Exchange(ref shared.writeCount, 0);
				int writeTimeoutCurrent = Interlocked.Exchange(ref shared.writeTimeoutCount, 0);
				int writeErrorCurrent = Interlocked.Exchange(ref shared.writeErrorCount, 0);
				int totalCount = shared.currentKey;
				
				DateTime time = DateTime.Now;
				double seconds = (double)time.Subtract(prevTime).TotalSeconds;
				prevTime = time;

				if (seconds > 0.0)
				{
					double writeTps = Math.Round((double)writeCurrent / seconds, 0);

					console.Info("write(tps={0} timeouts={1} errors={2} total={3})",
						writeTps, writeTimeoutCurrent, writeErrorCurrent, totalCount
					);

                    if (latencyHeader != null)
                    {
                        console.Write(latencyHeader);
                        console.Write(shared.writeLatency.PrintResults(latencyBuilder, "write"));
                    }
				}

				/* Keep going so node add/remove tests can be run.
				if (writeTimeoutCurrent + writeErrorCurrent > 10)
				{
                    if (GetIsStopWrites())
					{
						if (valid)
						{
							console.Error("Cluster is either in readonly mode or no nodes responded. Shutting down...");
							valid = false;
						}
					}
				}
				*/
				Thread.Sleep(1000);
			}
        }
    }
}
