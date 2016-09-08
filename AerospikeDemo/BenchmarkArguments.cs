/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
		internal bool requestProleReplicas;
		internal bool latency;
		internal bool debug;
		internal Value fixedValue;

        public void SetFixedValue()
        {
            // Fixed values are used when the extra random call overhead is not wanted
            // in the benchmark measurement.
			RandomShift random = new RandomShift();
            fixedValue = GetValue(random);
        }

		public Value GetValue(RandomShift random)
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
