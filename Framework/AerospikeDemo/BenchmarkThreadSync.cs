/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System.Threading;
using System.Diagnostics;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class BenchmarkThreadSync : BenchmarkThread
	{
		private AerospikeClient client;

        public BenchmarkThreadSync
        (
            Console console,
            BenchmarkArguments args,
            BenchmarkShared shared,
            Example example,
            AerospikeClient client
        ) : base(console, args, shared, example)
		{
			this.client = client;
		}

		protected override void WriteRecord(WritePolicy policy, Key key, Bin bin)
		{
            if (shared.writeLatency != null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                client.Put(policy, key, bin);
                double elapsed = watch.Elapsed.TotalMilliseconds;
                OnWriteSuccess(elapsed);
            }
            else
            {
                client.Put(policy, key, bin);
                OnWriteSuccess();
            }
		}

		protected override void ReadRecord(Policy policy, Key key, string binName)
		{
            if (shared.readLatency != null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                Record record = client.Get(policy, key, binName);
                double elapsed = watch.Elapsed.TotalMilliseconds;
                OnReadSuccess(elapsed);
            }
            else
            {
                Record record = client.Get(policy, key, binName);
                OnReadSuccess();
            }           
		}
	}
}
