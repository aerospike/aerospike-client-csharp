/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
	public class QueryString : SyncExample
	{
		public QueryString(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index and query on it.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}

			string indexName = "queryindex";
			string keyPrefix = "querykey";
			string valuePrefix = "queryvalue";
			string binName = args.GetBinName("querybin");
			int size = 5;

			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, valuePrefix, size);
			RunQuery(client, args, indexName, binName, valuePrefix);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, string valuePrefix, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, valuePrefix + i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName, string valuePrefix)
		{
			string filter = valuePrefix + 3;

			console.Info("Query for: ns={0} set={1} index={2} bin={3} filter={4}", 
				args.ns, args.set, indexName, binName, filter);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Equal(binName, filter));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					string result = (string)record.GetValue(binName);

					if (result.Equals(filter))
					{
						console.Info("Record found: namespace={0} set={1} digest={2} bin={3} value={4}", 
							key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), binName, result);
					}
					else
					{
						console.Error("Query mismatch: Expected {0}. Received {1}.", filter, result);
					}
					count++;
				}

				if (count == 0)
				{
					console.Error("Query failed. No records returned.");
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
