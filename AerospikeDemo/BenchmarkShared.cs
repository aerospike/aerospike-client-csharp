/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Linq;
using System.Text;

namespace Aerospike.Demo
{
    public class BenchmarkShared
    {
		internal LatencyManager writeLatency;
		internal int writeCount;
		internal int writeTimeoutCount;
		internal int writeErrorCount;

		internal LatencyManager readLatency;
		internal int readCount;
		internal int readTimeoutCount;
		internal int readErrorCount;

		internal int currentKey;

		public BenchmarkShared(BenchmarkArguments args)
		{
			if (args.latency)
			{
				writeLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
				readLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
			}
		}
    }
}
