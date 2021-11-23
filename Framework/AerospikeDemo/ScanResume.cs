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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ScanResume : SyncExample
	{
		private int recordCount;
		private int recordMax;

		public ScanResume(Console console) : base(console)
		{
		}

		/// <summary>
		/// Terminate a scan and then resume scan later.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			string binName = "bin";
			string setName = "resume";

			WriteRecords(client, args, setName, binName, 200);

			// Serialize node scans so scan callback atomics are not necessary.
			ScanPolicy policy = new ScanPolicy();
			policy.concurrentNodes = false;

			PartitionFilter filter = PartitionFilter.All();
			recordCount = 0;
			recordMax = 50;

			console.Info("Start scan terminate");

			try
			{
				client.ScanPartitions(policy, filter, args.ns, setName, ScanCallback);
			}
			catch (AerospikeException.ScanTerminated e)
			{
				console.Info("Scan terminated as expected");
			}
			console.Info("Records returned: " + recordCount);
			
			// PartitionFilter could be serialized at this point.
			// Resume scan now.
			recordCount = 0;
			recordMax = 0;

			console.Info("Start scan resume");
			client.ScanPartitions(policy, filter, args.ns, setName, ScanCallback);
			console.Info("Records returned: " + recordCount);
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
			if (recordMax > 0 && recordCount >= recordMax)
			{
				// Terminate scan. The scan last digest will not be set and the current record
				// will be returned again if the scan resumes at a later time.
				throw new AerospikeException.ScanTerminated();
			}

			recordCount++;
		}
	}
}
