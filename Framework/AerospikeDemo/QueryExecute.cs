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
	public class QueryExecute : SyncExample
	{
		public QueryExecute(Console console) : base(console)
		{
		}

		/// <summary>
		/// Apply user defined function on records that match the query filter.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			string indexName = "qeindex1";
			string keyPrefix = "qekey";
			string binName1 = args.GetBinName("qebin1");
			string binName2 = args.GetBinName("qebin2");
			int size = 10;

			Register(client, args);
			CreateIndex(client, args, indexName, binName1);
			WriteRecords(client, args, keyPrefix, binName1, binName2, size);
			RunQueryExecute(client, args, indexName, binName1, binName2);
			ValidateRecords(client, args, indexName, binName1, binName2, size);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "record_example.lua";
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

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName1, string binName2, int size)
		{
			console.Info("Write " + size + " records.");

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				client.Put(args.writePolicy, key, new Bin(binName1, i), new Bin(binName2, i));
			}
		}

		private void RunQueryExecute(AerospikeClient client, Arguments args, string indexName, string binName1, string binName2)
		{
			int begin = 3;
			int end = 9;

			console.Info("For ns={0} set={1} index={2} bin={3} >= {4} <= {5}", args.ns, args.set, indexName, binName1, begin, end);
			console.Info("Even integers: add 100 to existing " + binName1);
			console.Info("Multiple of 5: delete " + binName2 + " bin");
			console.Info("Multiple of 9: delete record");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			ExecuteTask task = client.Execute(args.writePolicy, stmt, "record_example", "processRecord", Value.Get(binName1), Value.Get(binName2), Value.Get(100));
			task.Wait(3000, 3000);
		}

		private void ValidateRecords(AerospikeClient client, Arguments args, string indexName, string binName1, string binName2, int size)
		{
			int begin = 1;
			int end = size + 100;

			console.Info("Validate records");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int[] expectedList = new int[] {1,2,3,104,5,106,7,108,-1,10};
				int expectedSize = size - 1;
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					object value1 = null;
					object value2 = null;

					record.bins.TryGetValue(binName1, out value1);
					record.bins.TryGetValue(binName2, out value2);

					console.Info("Record found: ns={0} set={1} bin1={2} value1={3} bin2={4} value2={5}", 
						key.ns, key.setName, binName1, value1, binName2, value2);

					if (value1 == null)
					{
						console.Error("Data mismatch. value1 is null");
						break;
					}
					long val1 = (long)value1;

					if (val1 == 9)
					{
						console.Error("Data mismatch. value1 " + val1 + " should not exist");
						break;
					}

					if (val1 == 5)
					{
						if (value2 != null)
						{
							console.Error("Data mismatch. value2 " + value2 + " should be null");
							break;
						}
					}
					else 
					{
						long val2 = (long)value2;
						
						if (val1 != expectedList[val2 - 1])
						{
							console.Error("Data mismatch. Expected " + expectedList[val2 - 1] + ". Received " + value1);
							break;
						}
					}
					count++;
				}

				if (count != expectedSize)
				{
					console.Error("Query count mismatch. Expected " + expectedSize + ". Received " + count);
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
