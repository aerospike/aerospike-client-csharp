using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Aerospike.Demo
{
    public sealed class LatencyManager
    {
        private readonly int[] buckets;
        private readonly int lastBucket;
        private readonly int bitShift;

        public LatencyManager(int columns, int bitShift)
        {
            this.lastBucket = columns - 1;
            this.bitShift = bitShift;
            buckets = new int[columns];
        }

        public void Add(double elapsed)
        {
            int index = GetIndex(elapsed);
            Interlocked.Increment(ref buckets[index]);
        }

        private int GetIndex(double elapsed)
        {
            int e = (int)Math.Ceiling(elapsed);
            int limit = 1;

            for (int i = 0; i < lastBucket; i++)
            {
                if (e <= limit)
                {
                    return i;
                }
                limit <<= bitShift;
            }
            return lastBucket;
        }

        public string PrintHeader()
        {
            StringBuilder sb = new StringBuilder(200);
            int limit = 1;
            sb.Append("      <=1ms >1ms");

            for (int i = 2; i <= lastBucket; i++)
            {
                limit <<= bitShift;
                String s = " >" + limit + "ms";
                sb.Append(s);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Print latency percents for specified cumulative ranges.
        /// This function is not absolutely accurate for a given time slice because this method 
        /// is not synchronized with the Add() method.  Some values will slip into the next iteration.  
        /// It is not a good idea to add extra locks just to measure performance since that actually 
        /// affects performance.  Fortunately, the values will even out over time
        /// (ie. no double counting).
        /// </summary>
        public string PrintResults(StringBuilder sb, string prefix)
        {
            // Capture snapshot and make buckets cumulative.
            int[] array = new int[buckets.Length];
            int sum = 0;
            int count;

            for (int i = buckets.Length - 1; i >= 1; i--)
            {
                count = Interlocked.Exchange(ref buckets[i], 0);
                array[i] = count + sum;
                sum += count;
            }
            // The first bucket (<=1ms) does not need a cumulative adjustment.
            count = Interlocked.Exchange(ref buckets[0], 0);
            array[0] = count;
            sum += count;

            // Print cumulative results.
            sb.Length = 0;
            sb.Append(prefix);
            int spaces = 6 - prefix.Length;

            for (int j = 0; j < spaces; j++)
            {
                sb.Append(' ');
            }

            double sumDouble = (double)sum;
            int limit = 1;

            PrintColumn(sb, limit, sumDouble, array[0]);
            PrintColumn(sb, limit, sumDouble, array[1]);

            for (int i = 2; i < array.Length; i++)
            {
                limit <<= bitShift;
                PrintColumn(sb, limit, sumDouble, array[i]);
            }
            return sb.ToString();
        }

        private void PrintColumn(StringBuilder sb, int limit, double sum, int value)
        {
            int percent = 0;

            if (value > 0)
            {
                percent = (int)((double)value * 100.0 / sum + 0.5);
            }
            string percentString = Convert.ToString(percent) + "%";
            int spaces = limit.ToString().Length + 4 - percentString.Length;

            for (int j = 0; j < spaces; j++)
            {
                sb.Append(' ');
            }
            sb.Append(percentString);
        }
    }
}
