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
using Aerospike.Client;
using System.Diagnostics;

namespace Aerospike.Benchmarks
{
	sealed class ReadWriteTaskSync : ReadWriteTask
	{
		private readonly AerospikeClient client;
		private readonly RandomShift random;
		private readonly Thread thread;

		public ReadWriteTaskSync(AerospikeClient client, Args args, Metrics metrics)
			: base(args, metrics)
		{
			this.client = client;
			this.random = new RandomShift();
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
				while (valid)
				{
					// Roll a percentage die.
					int die = random.Next(0, 100);

					if (die < args.readPct)
					{
						if (args.batchSize <= 1)
						{
							int key = random.Next(0, args.records);
							Read(key);
						}
						else
						{
							BatchRead();
						}
					}
					else
					{
						// Perform Single record write even if in batch mode.
						int key = random.Next(0, args.records);
						Write(key);
					}

					// Throttle throughput
					if (args.throughput > 0)
					{
						int transactions = metrics.writeCount + metrics.readCount;

						if (transactions > args.throughput)
						{
							long millis = metrics.TimeRemaining;

							if (millis > 0)
							{
								Util.Sleep((int)millis);
							}
						}
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("ReadWriteTaskSync error: " + e.Message + System.Environment.NewLine + e.StackTrace);
			}
		}

		private void Write(int userKey)
		{
			Key key = new Key(args.ns, args.set, userKey);
			Bin bin = new Bin(args.binName, args.GetValue(random));

			try
			{
				WriteRecord(args.writePolicy, key, bin);
			}
			catch (AerospikeException ae)
			{
				metrics.WriteFailure(ae);
			}
			catch (Exception e)
			{
				metrics.WriteFailure(e);
			}
		}

		private void WriteRecord(WritePolicy policy, Key key, Bin bin)
		{
			if (metrics.writeLatency != null)
			{
				Stopwatch watch = Stopwatch.StartNew();
				client.Put(policy, key, bin);
				long elapsed = watch.ElapsedMilliseconds;
				metrics.WriteSuccess(elapsed);
			}
			else
			{
				client.Put(policy, key, bin);
				metrics.WriteSuccess();
			}
		}

		private void Read(int userKey)
		{
			Key key = new Key(args.ns, args.set, userKey);

			try
			{
				ReadRecord(args.writePolicy, key, args.binName);
			}
			catch (AerospikeException ae)
			{
				metrics.ReadFailure(ae);
			}
			catch (Exception e)
			{
				metrics.ReadFailure(e);
			}
		}

		private void ReadRecord(Policy policy, Key key, string binName)
		{
			if (metrics.readLatency != null)
			{
				Stopwatch watch = Stopwatch.StartNew();
				Record record = client.Get(policy, key, binName);
				long elapsed = watch.ElapsedMilliseconds;
				metrics.ReadSuccess(elapsed);
			}
			else
			{
				Record record = client.Get(policy, key, binName);
				metrics.ReadSuccess();
			}
		}

		private void BatchRead()
		{
			Key[] keys = new Key[args.batchSize];

			for (int i = 0; i < keys.Length; i++)
			{
				long keyIdx = random.Next(0, args.records);
				keys[i] = new Key(args.ns, args.set, keyIdx);
			}

			try
			{
				BatchRead(args.batchPolicy, keys, args.binName);
			}
			catch (AerospikeException ae)
			{
				metrics.ReadFailure(ae);
			}
			catch (Exception e)
			{
				metrics.ReadFailure(e);
			}
		}

		private void BatchRead(BatchPolicy policy, Key[] keys, string binName)
		{
			if (metrics.readLatency != null)
			{
				Stopwatch watch = Stopwatch.StartNew();
				Record[] records = client.Get(policy, keys, binName);
				long elapsed = watch.ElapsedMilliseconds;
				metrics.ReadSuccess(elapsed);
			}
			else
			{
				Record[] records = client.Get(policy, keys, binName);
				metrics.ReadSuccess();
			}
		}
	}
}