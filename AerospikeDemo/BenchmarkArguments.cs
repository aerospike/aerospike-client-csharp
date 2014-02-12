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
