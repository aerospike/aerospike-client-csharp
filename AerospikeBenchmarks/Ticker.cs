using Aerospike.Benchmarks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aerospike.Benchmarks
{
    internal sealed class Ticker
    {
        public Ticker(Args args, Metrics metrics, ILatencyManager latencyManager)
        {
            this.Args = args;
            this.Metrics = metrics;
            this.LatencyManager = latencyManager;
        }

        public Args Args { get; }
        public Metrics Metrics { get; }
        public ILatencyManager LatencyManager { get; }
        public StringBuilder LatencyBuilder { get; private set; }
        public string LatencyHeader { get; private set; }

        public void Run()
        {
           
            if (Metrics.Type == Metrics.MetricTypes.Write)
            {
                LatencyBuilder = new StringBuilder(200);
                LatencyHeader = LatencyManager.PrintHeader();
            }

            Timer = new Timer(TimerCallBack,
                                (Metrics, LatencyManager, LatencyHeader, LatencyBuilder),
                                Timeout.Infinite,
                                Timeout.Infinite);

            Interlocked.Exchange(ref TimerEntry, 0);
            Timer.Change(TimerInterval, Timeout.Infinite);            
        }

        public void WaitForAllToPrint()
        {
            if (!StopTimer)
            {
                Timer.Change(Timeout.Infinite, Timeout.Infinite);

                if (!StopTimer
                        && Interlocked.Read(ref TimerEntry) == 0 //Not running
                        && this.Metrics.CurrentCounters.Count > 0) //We have something to report
                {
                    TimerCallBack((Metrics, LatencyManager, LatencyHeader, LatencyBuilder));
                }

                this.Stop();
            }
        }

        public void Stop()
        {
            StopTimer = true;

            if (Metrics.Type == Metrics.MetricTypes.Write)
            {
                Console.WriteLine("Latency Summary");

                if (LatencyHeader != null)
                {
                    Console.WriteLine(LatencyHeader);
                }
                Console.WriteLine(LatencyManager.PrintSummary(LatencyBuilder, "write"));
            }
        }

        public static Timer Timer { get; private set; }

        static long TimerEntry = 0;
        static bool StopTimer = false;
        public static int TimerInterval = 1000;

        private static void TimerCallBack(object state)
        {
            var item = ((Metrics metrics,
                             ILatencyManager latencyManager,
                             string latencyHdr,
                             StringBuilder latencyBuilder))state;

            if (StopTimer)
            {
                Interlocked.Exchange(ref TimerEntry, 0);
                Timer.Change(Timeout.Infinite, Timeout.Infinite);
                return;
            }

            if (Interlocked.Read(ref TimerEntry) > 0)
            {
                Timer.Change(TimerInterval, TimerInterval);
                return;
            }

            Interlocked.Increment(ref TimerEntry);

            try
            {
                long time = item.metrics.AppRunningTime;
                var periodBlock = item.metrics.NewBlockCounter();

                if (periodBlock.Count > 0)
                {
                    string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                    Console.WriteLine($"{dt} write(count={periodBlock.Count:###,###,##0} tps={periodBlock.TPS():###,###,##0} timeouts={periodBlock.TimeoutCount} errors={periodBlock.ErrorCount})");

                    if (item.metrics.Type == Metrics.MetricTypes.Write)
                    {
                        if (item.latencyHdr != null)
                        {
                            Console.WriteLine(item.latencyHdr);
                        }
                        Console.WriteLine(item.latencyManager.PrintResults(item.latencyBuilder, item.metrics.Type.ToString()));
                    }
                }
            }
            finally
            {
                Interlocked.Decrement(ref TimerEntry);
                Timer.Change(TimerInterval, Timeout.Infinite);
            }
        }
    }
}
