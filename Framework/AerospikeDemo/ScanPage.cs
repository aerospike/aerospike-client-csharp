/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ScanPage : SyncExample
	{
		private int recordCount = 0;

		public ScanPage(Console console) : base(console)
		{
		}

		/// <summary>
		/// Scan in pages.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			string binName = "bin";
			string setName = "page";

			WriteRecords(client, args, setName, binName, 200);

			ScanPolicy policy = new ScanPolicy();
			policy.maxRecords = 100;

			PartitionFilter filter = PartitionFilter.All();

			// Scan 3 pages of records.
			for (int i = 0; i < 3 && !filter.Done; i++)
			{
				recordCount = 0;

				console.Info("Scan page: " + i);
				client.ScanPartitions(policy, filter, args.ns, setName, ScanCallback);
				console.Info("Records returned: " + recordCount);
			}
		}

		private void WriteRecords
		(
			AerospikeClient client,
			Arguments args,
			string setName,
			string binName,
			int size
		)
		{
			console.Info("Write " + size + " records.");

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, setName, i);
				Bin bin = new Bin(binName, i);
				client.Put(args.writePolicy, key, bin);
			}
		}

		public void ScanCallback(Key key, Record record)
		{
			// Callbacks must ensure thread safety when ScanAll() is used with ScanPolicy
			// concurrentNodes set to true (default).  In this case, parallel
			// node threads will be sending data to this callback.
			Interlocked.Increment(ref recordCount);
		}
	}
}
