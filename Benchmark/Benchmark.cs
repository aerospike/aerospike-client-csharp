/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Neo.IronLua;

namespace Benchmarks
{
	[SimpleJob(RunStrategy.Monitoring, launchCount: 1,
		warmupCount: 2, iterationCount: 10)]
	public class Benchmark
	{
		public IAerospikeClientNew asyncAwaitClient;
		public IAerospikeClient nativeClient;
		public IAsyncClient nativeAsyncClient;
		public WriteListener writeListener;
		public RecordListener recordListener;

		public IEnumerable<int> pkRange => Enumerable.Range(1, 100000);

		[GlobalSetup]
		public void SetUp()
		{
			SetUpClient("localhost", 3000, "charlie", "123456");
		}

		public void SetUpClient(string host, int port, string user, string password)
		{
			ClientPolicy policy = new ClientPolicy();
			AsyncClientPolicy asyncPolicy = new AsyncClientPolicy();

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				asyncPolicy.user = user;
				policy.password = password;
				asyncPolicy.password = password;
			}

			asyncAwaitClient = new AerospikeClientNew(policy, host, port);
			nativeClient = new AerospikeClient(policy, host, port);
			nativeAsyncClient = new AsyncClient(asyncPolicy, host, port);

			writeListener = new WriteHandler();
			recordListener = new RecordHandler();
		}

		[Benchmark]
		public void AsyncAwaitPut()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "aa" + pk);
				var bin = new Bin("bin", pk);
				asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None).Wait();
			}
		}

		[Benchmark]
		public async Task AsyncAwaitPutAsync()
		{
			await Parallel.ForEachAsync(pkRange,
										CancellationToken.None,
					async (pk, token) =>
			{
				var key = new Key("test", "test", "aaa" + pk);
				var bin = new Bin("bin", pk);
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);
			});
		}

		[Benchmark]
		public void AsyncAwaitGet()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "aa" + pk);
				asyncAwaitClient.Get(null, key, CancellationToken.None).Wait();
			}
		}

		[Benchmark]
		public async Task AsyncAwaitGetAsync()
		{
			await Parallel.ForEachAsync(pkRange,
										CancellationToken.None,
				async (pk, token) =>
			{
				var key = new Key("test", "test", "aaa" + pk);
				await asyncAwaitClient.Get(null, key, CancellationToken.None);
			});
		}

		[Benchmark]
		public void NativePut()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "n" + pk);
				var bin = new Bin("bin", pk);
				nativeClient.Put(null, key, bin);
			}
		}

		[Benchmark]
		public void NativeGet()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "n" + pk);
				nativeClient.Get(null, key);
			}
		}

		[Benchmark]
		public void NativeAsyncPut()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "na" + pk);
				var bin = new Bin("bin", pk);
				nativeAsyncClient.Put(null, writeListener, key, bin);
			}
		}

		[Benchmark]
		public void NativeAsyncGet()
		{
			foreach (var pk in pkRange)
			{
				var key = new Key("test", "test", "na" + pk);
				nativeAsyncClient.Get(null, recordListener, key);
			}
		}

		private class WriteHandler : WriteListener
		{
			public WriteHandler()
			{
			}

			public void OnSuccess(Key key)
			{
			}

			public void OnFailure(AerospikeException e)
			{
			}
		}

		private class RecordHandler : RecordListener
		{
			public RecordHandler()
			{
			}

			public void OnSuccess(Key key, Record record)
			{
			}

			public void OnFailure(AerospikeException e)
			{
			}
		}
	}
}
