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
using System.Diagnostics;
using Aerospike.Client;

namespace Aerospike.Benchmarks
{
	sealed class WriteTaskSync
	{
		private readonly AerospikeClient client;
		private readonly Args args;
		private readonly Metrics metrics;
		private readonly long keyStart;
		private readonly long keyMax;
		private readonly Thread thread;
		private readonly ILatencyManager LatencyMgr;
		private readonly bool useLatency;

		public WriteTaskSync(AerospikeClient client, 
								Args args, 
								Metrics metrics, 
								long keyStart, 
								long keyMax,
								ILatencyManager latencyManager)
		{
			this.client = client;
			this.args = args;
			this.metrics = metrics;
			this.keyStart = keyStart;
			this.keyMax = keyMax;
			this.LatencyMgr = latencyManager;
			this.useLatency = latencyManager != null;
			this.thread = new Thread(new ThreadStart(Run));
		}

		public void Start()
		{
			thread.Start();
		}

		public void Run()
		{
			try
			{
				RandomShift random = new RandomShift();

				for (long i = 0; i < keyMax; i++)
				{
					try
					{
						Write(keyStart + i, random);
					}
					catch (AerospikeException ae)
					{
						i--;
						metrics.Failure(ae);
					}
					catch (Exception e)
					{
						i--;
						metrics.Failure(e);
					}

					// Throttle throughput
					if (args.throughput > 0)
					{
						long transactions = Volatile.Read(ref metrics.Counters.Count);

						/*if (transactions > args.throughput)
						{
							long millis = metrics.TimeRemaining;

							if (millis > 0)
							{
								Util.Sleep((int)millis);
							}
						}*/
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Insert task error: " + e.Message + System.Environment.NewLine + e.StackTrace);
			}
		}

		private void Write(long keyCurrent, RandomShift random)
		{
			Key key = new Key(args.ns, args.set, keyCurrent);
			Bin bin = new Bin(args.binName, args.GetValue(random));

			if (useLatency)
			{
				Stopwatch watch = useLatency
								? Stopwatch.StartNew()
								: null;
				client.Put(args.writePolicy, key, bin);
				PrefStats.StopRecording(watch,
										metrics.Type.ToString(),
										nameof(Write),
										key);

				var elapsed = watch.Elapsed;
				this.metrics.Success(elapsed);
				this.LatencyMgr?.Add((long)elapsed.TotalMilliseconds);
			}
			else
			{
				client.Put(args.writePolicy, key, bin);
				metrics.Success();
			}
		}
	}
}