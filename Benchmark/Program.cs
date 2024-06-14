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
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Hosting;

namespace Benchmarks
{
	class Program
	{
		static void Main(string[] args)
		{
			string host = "localhost";
			int port = 3000;
			string user = "charlie";
			string password = "123456";
			ClientPolicy policy = new ClientPolicy();

			if (user != null && user.Length > 0)
			{
				policy.user = user;
				policy.password = password;
			}

			var client = new AerospikeClient(policy, host, port);
			client.Truncate(null, "test", "test", null);

			var summary = BenchmarkRunner.Run<Benchmark>();
		}
	}
}
