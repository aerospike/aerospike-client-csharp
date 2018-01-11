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
using System.IO;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QuerySum : SyncExample
	{
		public QuerySum(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index and query on it and apply aggregation user defined function.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}
			string indexName = "aggindex";
			string keyPrefix = "aggkey";
			string binName = args.GetBinName("aggbin");
			int size = 10;

			Register(client, args);
			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, size);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "sum_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
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

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			int begin = 4;
			int end = 7;

			console.Info("Query for:ns={0} set={1} index={2} bin={3} >= {4} <= {5}", 
				args.ns, args.set, indexName, binName, begin, end);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			ResultSet rs = client.QueryAggregate(null, stmt, "sum_example", "sum_single_bin", Value.Get(binName));

			try
			{
				int expected = 22; // 4 + 5 + 6 + 7
				int count = 0;

				while (rs.Next())
				{
					object obj = rs.Object;

					if (obj is long)
					{
						long sum = (long)rs.Object;

						if (expected == (int)sum)
						{
							console.Info("Sum matched: value=" + expected);
						}
						else
						{
							console.Error("Sum mismatch: Expected {0}. Received {1}.", expected, sum);
						}
					}
					else
					{
						console.Error("Unexpected return value: " + obj);
						continue;
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
