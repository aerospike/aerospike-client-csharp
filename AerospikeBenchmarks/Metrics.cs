/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Diagnostics;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Benchmarks
{
	sealed class Metrics
	{
		private readonly Args args;
		private readonly Stopwatch watch;
		private long periodBegin;

		internal readonly ILatencyManager writeLatency;
		internal int writeCount;
		internal int writeTimeoutCount;
		internal int writeErrorCount;

		internal readonly ILatencyManager readLatency;
		internal int readCount;
		internal int readTimeoutCount;
		internal int readErrorCount;

		public Metrics(Args args)
		{
			this.args = args;

			if (args.latency)
			{
				if (args.latencyAltFormat)
				{
					writeLatency = new LatencyManagerAlt(args.latencyColumns, args.latencyShift);
					readLatency = new LatencyManagerAlt(args.latencyColumns, args.latencyShift);
				}
				else
				{
					writeLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
					readLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
				}
			}
			watch = Stopwatch.StartNew();
		}

		public void Start()
		{
			periodBegin = Time;
		}

		public long NextPeriod(long time)
		{
			long elapsed = time - periodBegin;
			periodBegin = time;
			return elapsed;
		}

		public long TimeRemaining
		{
			get { return Volatile.Read(ref periodBegin) + 1000L - Time; }
		}

		public long Time
		{
			get { return watch.ElapsedMilliseconds; }
		}

		public void WriteSuccess(long elapsed)
		{
			Interlocked.Increment(ref writeCount);
			writeLatency.Add(elapsed);
		}

		public void WriteSuccess()
		{
			Interlocked.Increment(ref writeCount);
		}

		public void WriteFailure(AerospikeException ae)
		{
			if (ae.Result == ResultCode.TIMEOUT)
			{
				Interlocked.Increment(ref writeTimeoutCount);
			}
			else
			{
				WriteFailure((Exception)ae);
			}
		}

		public void WriteFailure(Exception e)
		{
			Interlocked.Increment(ref writeErrorCount);

			if (args.debug)
			{
				Console.WriteLine("Write error: " + e.Message + System.Environment.NewLine + e.StackTrace);
			}
		}

		public void ReadSuccess(long elapsed)
		{
			Interlocked.Increment(ref readCount);
			readLatency.Add(elapsed);
		}

		public void ReadSuccess()
		{
			Interlocked.Increment(ref readCount);
		}

		public void ReadFailure(AerospikeException ae)
		{
			if (ae.Result == ResultCode.TIMEOUT)
			{
				Interlocked.Increment(ref readTimeoutCount);
			}
			else
			{
				ReadFailure((Exception)ae);
			}
		}

		public void ReadFailure(Exception e)
		{
			Interlocked.Increment(ref readErrorCount);

			if (args.debug)
			{
				Console.WriteLine("Read error: " + e.Message + System.Environment.NewLine + e.StackTrace);
			}
		}
	}
}
