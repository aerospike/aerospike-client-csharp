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
				int writeFailCurrent = Interlocked.Exchange(ref shared.writeFailCount, 0);
                int totalCount = shared.currentKey;
				
				DateTime time = DateTime.Now;
				double seconds = (double)time.Subtract(prevTime).TotalSeconds;
				prevTime = time;

				if (seconds > 0.0)
				{
					double writeTps = Math.Round((double)writeCurrent / seconds, 0);

					console.Info("write(tps={0} fail={1} total={2}))",
						writeTps, writeFailCurrent, totalCount
					);

                    if (latencyHeader != null)
                    {
                        console.Write(latencyHeader);
                        console.Write(shared.writeLatency.PrintResults(latencyBuilder, "write"));
                    }
                    prevTime = time;
				}

				if (writeFailCurrent > 10)
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
