using System;
using System.Diagnostics;

namespace Aerospike.Client
{
    /// <summary>
    /// <see cref="Stopwatch"/> but as a struct.
    /// </summary>
    internal struct ValueStopwatch
    {
        private static readonly double TimestampTicksPerMachineTicks = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;
        private static readonly double TimestampTicksPerMilliseconds = TimestampTicksPerMachineTicks / TimeSpan.TicksPerMillisecond;

        public static ValueStopwatch StartNew() => new ValueStopwatch(Stopwatch.GetTimestamp());

        private readonly long startTimestamp;

        public bool IsRunning => startTimestamp != 0;

        public int ElapsedMilliseconds
        {
            get
            {
                if (!IsRunning)
                {
                    throw new InvalidOperationException("Cannot get elapsed time of a non-running stopwatch");
                }

                long endTimestamp = Stopwatch.GetTimestamp();
                long durationTimestamp = endTimestamp - startTimestamp;
                return (int)(durationTimestamp * TimestampTicksPerMilliseconds);
            }
        }

        private ValueStopwatch(long startTimestamp) => this.startTimestamp = startTimestamp;
    }
}
