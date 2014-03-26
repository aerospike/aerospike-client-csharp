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

				if (writeTimeoutCurrent + writeErrorCurrent > 10)
				{
                    if (GetIsStopWrites())
					{
						if (valid)
						{
							console.Error("Server is currently in readonly mode. Shutting down...");
                            valid = false;
						}
					}
				}
				Thread.Sleep(1000);
			}
        }
    }
}
