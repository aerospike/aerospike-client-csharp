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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryInteger : SyncExample
	{
		public QueryInteger(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index on an integer bin and query on it.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			string indexName = "queryindexint";
			string keyPrefix = "querykeyint";
			string binName = args.GetBinName("querybinint");
			int size = 50;

			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, size);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void CreateIndex(IAerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}
		}

		private void WriteRecords(IAerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
		{
			console.Info("Write " + size + " records.");

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);
				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(IAerospikeClient client, Arguments args, string indexName, string binName)
		{
			int begin = 14;
			int end = 18;

			console.Info("Query for: ns={0} set={1} index={2} bin={3} >= {4} <= {5}", 
				args.ns, args.set, indexName, binName, begin, end);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					long result = record.GetLong(binName);

					console.Info("Record found: namespace={0} set={1} digest={2} bin={3} value={4}",
						key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), binName, result);

					count++;
				}

				if (count != 5)
				{
					console.Error("Query count mismatch. Expected 5. Received " + count);
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
