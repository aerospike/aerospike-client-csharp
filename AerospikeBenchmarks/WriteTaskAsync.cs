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
	sealed class WriteTaskAsync
	{
		private readonly AsyncClient client;
		private readonly Args args;
		private readonly Metrics metrics;
		private readonly RandomShift random;
		private readonly WriteListener listener;
		private readonly Stopwatch watch;
		private readonly long keyStart;
		private readonly long keyMax;
		private long keyCount;
		private long begin;
		private readonly bool useLatency;

		public WriteTaskAsync(AsyncClient client, Args args, Metrics metrics, long keyStart, long keyMax)
		{
			this.client = client;
			this.args = args;
			this.metrics = metrics;
			this.random = new RandomShift();
			this.keyStart = keyStart;
			this.keyMax = keyMax;
			this.useLatency = metrics.writeLatency != null;

			if (useLatency)
			{
				listener = new LatencyWriteHandler(this);
			}
			else
			{
				listener = new WriteHandler(this);
			}
			watch = Stopwatch.StartNew();
		}

		public void Start()
		{
			RunCommand(keyCount);
		}

		public void RunCommand(long count)
		{
			long currentKey = keyStart + count;
			Key key = new Key(args.ns, args.set, currentKey);
			Bin bin = new Bin(args.binName, args.GetValue(random));

			if (useLatency)
			{
				begin = watch.ElapsedMilliseconds;
			}
			client.Put(args.writePolicy, listener, key, bin);
		}

		private class LatencyWriteHandler : WriteListener
		{
			private readonly WriteTaskAsync parent;

			public LatencyWriteHandler(WriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key)
			{
				parent.WriteSuccessLatency();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.WriteFailure(ae);
			}
		}

		private class WriteHandler : WriteListener
		{
			private readonly WriteTaskAsync parent;

			public WriteHandler(WriteTaskAsync parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key k)
			{
				parent.WriteSuccess();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.WriteFailure(ae);
			}
		}

		private void WriteSuccessLatency()
		{
			long elapsed = watch.ElapsedMilliseconds - Volatile.Read(ref begin);
			metrics.writeLatency.Add(elapsed);
			WriteSuccess();
		}

		private void WriteSuccess()
		{
			metrics.WriteSuccess();
			long count = Interlocked.Increment(ref keyCount);

			if (count < keyMax)
			{
				// Try next command.
				RunCommand(count);
			}
		}

		private void WriteFailure(AerospikeException ae)
		{
			metrics.WriteFailure(ae);
			// Retry command with same key.
			RunCommand(keyCount);
		}
	}
}