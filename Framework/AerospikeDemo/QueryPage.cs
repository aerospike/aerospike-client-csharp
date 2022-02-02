/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	public class QueryPage : SyncExample
	{
		public QueryPage(Console console) : base(console)
		{
		}

		/// <summary>
		/// Query in pages.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			string indexName = "pqidx";
			string binName = "bin";
			string setName = "pq";

			CreateIndex(client, args, setName, indexName, binName);
			WriteRecords(client, args, setName, binName, 190);

			Statement stmt = new Statement();
			stmt.Namespace = args.ns;
			stmt.SetName = setName;
			stmt.BinNames = new string[] {binName};
			stmt.Filter = Filter.Range(binName, 70, 150);
			stmt.MaxRecords = 50;

			PartitionFilter filter = PartitionFilter.All();

			// Query 3 pages of records.
			for (int i = 0; i < 3 && !filter.Done; i++)
			{
				console.Info("Query page: " + i);

				RecordSet rs = client.QueryPartitions(null, stmt, filter);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}

					console.Info("Records returned: " + count);
				}
				finally
				{
					rs.Close();
				}
			}
			client.DropIndex(args.policy, args.ns, setName, indexName);
		}

		private void CreateIndex
		(
			AerospikeClient client,
			Arguments args,
			string setName,
			string indexName,
			string binName
		)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, setName, indexName, binName);

			Policy policy = new Policy();

			try
			{
				IndexTask task = client.CreateIndex(policy, args.ns, setName, indexName, binName, IndexType.NUMERIC);
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
	}
}
