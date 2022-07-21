using System;
using System.Diagnostics;

namespace Aerospike.Client
{
    /// <summary>
    /// <see cref="Stopwatch"/> but as a struct.
    /// </summary>
    internal readonly struct ValueStopwatch
    {
        private static readonly double TimestampTicksToMachineTicksRatio = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;

        public static ValueStopwatch StartNew() => new ValueStopwatch(Stopwatch.GetTimestamp());

        private readonly long startTimestamp;

        public TimeSpan Elapsed => new TimeSpan((long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampTicksToMachineTicksRatio));

        private ValueStopwatch(long startTimestamp) => this.startTimestamp = startTimestamp;
    }
}
