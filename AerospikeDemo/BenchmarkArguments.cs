using System;
using System.Collections.Generic;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Demo
{
	/// <summary>
	/// Benchmark configuration data.
	/// </summary>
	public class BenchmarkArguments : Arguments
	{
        internal int threadMax;
        internal int records;
        internal int recordsInit;
        internal BinType binType;
        internal int binSize;
        internal int readPct;
        internal int latencyColumns;
        internal int latencyShift;
		internal bool sync;
        internal bool latency;
		internal bool debug;
        internal Value fixedValue;

        public void SetFixedValue()
        {
            // Fixed values are used when the extra random call overhead is not wanted
            // in the benchmark measurement.
            Random random = new Random();
            fixedValue = GetValue(random);
        }

        public Value GetValue(Random random)
        {
            if (fixedValue != null)
            {
                return fixedValue;
            }

            // Generate random value.
            switch (binType)
            {
                case BinType.Integer:
                    return Value.Get(random.Next());

                case BinType.String:
                    StringBuilder sb = new StringBuilder(binSize);

                    for (int i = 0; i < binSize; i++)
                    {
                        sb.Append((char) random.Next(33,127));
                    }
                    return Value.Get(sb.ToString());

                case BinType.Byte:
                    byte[] bytes = new byte[binSize];
                    random.NextBytes(bytes);
                    return Value.Get(bytes);

                default:
                    return null;
            }
        }
	}

    public enum BinType
    {
        Integer,
        String,
        Byte
    }
}