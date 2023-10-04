/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System.Threading;

namespace Aerospike.Demo
{
	public class QueryResume : SyncExample
	{
		private int recordCount;
		private int recordMax;

		public QueryResume(Console console) : base(console)
		{
		}

		/// <summary>
		/// Terminate a query and then resume query later.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			string indexName = "qridx";
			string binName = "bin";
			string setName = "qr";

			CreateIndex(client, args, setName, indexName, binName);
			WriteRecords(client, args, setName, binName, 200);

			Statement stmt = new Statement();
			stmt.Namespace = args.ns;
			stmt.SetName = setName;
			stmt.BinNames = new string[] { binName };
			stmt.Filter = Filter.Range(binName, 1, 200);

			PartitionFilter filter = PartitionFilter.All();

			console.Info("Start query");
			recordCount = 0;
			recordMax = 50;

			try
			{
				client.Query(null, stmt, filter, QueryListener);
			}
			catch (AerospikeException.QueryTerminated)
			{
				console.Info("Query terminated as expected");
			}
			console.Info("Records returned: " + recordCount);

			// PartitionFilter could be serialized at this point.
			// Resume query now.
			recordCount = 0;
			recordMax = 0;

			console.Info("Start query resume");
			client.Query(null, stmt, filter, QueryListener);
			console.Info("Records returned: " + recordCount);

			client.DropIndex(args.policy, args.ns, setName, indexName);
		}

		private void CreateIndex
		(
			IAerospikeClient client,
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
			IAerospikeClient client,
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

		public void QueryListener(Key key, Record record)
		{
			int count = Interlocked.Increment(ref recordCount);

			if (recordMax > 0 && count >= recordMax)
			{
				// Terminate query. The query last record key will not be set
				// and the current record will be returned again if the query resumes
				// at a later time. It's designed this way to handle errors where
				// the last record returned could not be processed (like a disk full
				// error on a backup).
				throw new AerospikeException.QueryTerminated();
			}
		}
	}
}
