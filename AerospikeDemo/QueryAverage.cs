/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections.Generic;
using System.IO;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryAverage : SyncExample
	{
		public QueryAverage(Console console) : base(console)
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
			string indexName = "avgindex";
			string keyPrefix = "avgkey";
			string binName = args.GetBinName("l2");
			int size = 10;

			Register(client, args);
			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, size);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "average_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin("l1", i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}",
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, key, bin, new Bin("l2", 1));
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Query for:ns={0} set={1} index={2} bin={3}", 
				args.ns, args.set, indexName, binName);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilters(Filter.Equal(binName, 1));

			ResultSet rs = client.QueryAggregate(null, stmt, "average_example", "average");

			try
			{
				if (rs.Next())
				{
					object obj = rs.Object;

					if (obj is Dictionary<object,object>)
					{
						Dictionary<object, object> map = (Dictionary<object, object>)obj;
						object objsum = map["sum"];
						object objcount = map["count"];
						double sum = (double)(long)objsum;
						double count = (double)(long)objcount;
						double avg = sum / count;
						console.Info("Sum=" + sum + " Count=" + count + " Average=" + avg);

						double expected = 5.5;
						if (avg != expected)
						{
							console.Error("Data mismatch: Expected {0}. Received {1}.", expected, avg);
						}
					}
					else
					{
						console.Error("Unexpected object returned: " + obj);
					}
				}
				else
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
