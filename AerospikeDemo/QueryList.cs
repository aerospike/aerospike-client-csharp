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
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryList : SyncExample
	{
		public QueryList(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index and query on list bins.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			string indexName = "qlindex";
			string keyPrefix = "qlkey";
			string binName = "listbin";
			int size = 20;

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
				IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING, IndexCollectionType.LIST);
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
			console.Info("Write records");
			Random random = new Random();

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);

				List<String> list = new List<String>();
				list.Add(random.Next(900, 910).ToString());
				list.Add(random.Next(900, 910).ToString());
				list.Add(random.Next(900, 910).ToString());
				
				Bin bin = new Bin(binName, list);

				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(IAerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Query list bins");

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Contains(binName, IndexCollectionType.LIST, "905"));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					count++;
					Key key = rs.Key;
					Record record = rs.Record;
					IList list = record.GetList(binName);

					console.Info("Record " + count);

					foreach (string s in list)
					{
						console.Info(s);
					}
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
