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
using System.Net;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Benchmarks
{
	sealed public class Metrics
	{
		public enum MetricTypes
		{
			None,
			Read,
			Write
		}

		public struct BlockCounters
		{
			public long Count;
			public long TimeoutCount;
			public long ErrorCount;
			public long TimingTicks;

			public double TPS() => TimingTicks == 0
									? 0d
									: this.Count / TimeSpan.FromTicks(this.TimingTicks).TotalSeconds;

		}

		private readonly Args Args;
		private readonly Stopwatch AppStopWatch;
		public readonly MetricTypes Type;

		public BlockCounters Counters;

		public long TotalCount;
		public long TotalTicks;

		internal Metrics(MetricTypes type, Args args)
		{
			this.Args = args;
			this.Type = type;

			AppStopWatch = Stopwatch.StartNew();
		}

		public long AppRunningTime
		{
			get => AppStopWatch.ElapsedMilliseconds;
		}

		/// <summary>
		/// Returns the old block counter...
		/// </summary>
		/// <returns></returns>
		public BlockCounters NewBlockCounter()
		{
			BlockCounters newBlock = new();
			var oldBlock = this.Counters;

			this.Counters = newBlock;

			Interlocked.Add(ref this.TotalCount, oldBlock.Count);
			Interlocked.Add(ref this.TotalTicks, oldBlock.TimingTicks);

			return oldBlock;
		}

		public BlockCounters CurrentCounters { get => this.Counters; }


		public void Success(TimeSpan elapsed)
		{
			Interlocked.Increment(ref this.Counters.Count);
			Interlocked.Add(ref this.Counters.TimingTicks, elapsed.Ticks);
		}

		public void Success() => Interlocked.Increment(ref this.Counters.Count);

		public void Failure(AerospikeException ae)
		{
			if (ae.Result == ResultCode.TIMEOUT)
			{
				Interlocked.Increment(ref this.Counters.TimeoutCount);
			}
			else
			{
				Failure((Exception)ae);
			}
		}

		public void Failure(Exception e)
		{
			Interlocked.Increment(ref this.Counters.ErrorCount);

			if (Args.debug)
			{
				if (e is AggregateException ae)
				{
					ae.Handle(ex =>
					{
						System.Diagnostics.Debug.WriteLine("Write error: " + ex.Message + System.Environment.NewLine + ex.StackTrace);
						Console.WriteLine("Write error: " + ex.Message + System.Environment.NewLine + ex.StackTrace);
						return true;
					});
				}
				else
				{
					System.Diagnostics.Debug.WriteLine("Write error: " + e.Message + System.Environment.NewLine + e.StackTrace);
					Console.WriteLine("Write error: " + e.Message + System.Environment.NewLine + e.StackTrace);
				}
			}
		}


	}
}
