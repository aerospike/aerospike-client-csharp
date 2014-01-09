using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkReadWrite : BenchmarkExample
	{
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
				int readCurrent = Interlocked.Exchange(ref shared.readCount, 0);
                int readFailCurrent = Interlocked.Exchange(ref shared.readFailCount, 0);
				
				DateTime time = DateTime.Now;
				double seconds = (double)time.Subtract(prevTime).TotalSeconds;
				prevTime = time;

				if (seconds > 0.0)
				{
					double writeTps = Math.Round((double)writeCurrent / seconds, 0);
					double readTps = Math.Round((double)readCurrent / seconds, 0);

					console.Info("write(tps={0} fail={1}) read(tps={2} fail={3}) total(tps={4} fail={5})",
						writeTps, writeFailCurrent, readTps, readFailCurrent,
						writeTps + readTps, writeFailCurrent + readFailCurrent);

                    if (latencyHeader != null)
                    {
                        console.Write(latencyHeader);
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
