/* 
 * Copyright 2012-2026 Aerospike, Inc.
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

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryResume : TestSync
	{
		private const string setName = "qr";
		private const string indexName = "qridx";
		private const string binName = "bin";
		private const int recordCount = 200;
		private const int terminateAfter = 50;

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0
			};

			try
			{
				IndexTask task = client.CreateIndex(policy, SuiteHelpers.ns, setName, indexName, binName, IndexType.INTEGER);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= recordCount; i++)
			{
				Key key = new(SuiteHelpers.ns, setName, i);
				client.Put(null, key, new Bin(binName, i));
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, setName, indexName);

			for (int i = 1; i <= recordCount; i++)
			{
				client.Delete(null, new Key(SuiteHelpers.ns, setName, i));
			}
		}

		[TestMethod]
		public void QueryResume()
		{
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = setName,
				BinNames = [binName],
				Filter = Filter.Range(binName, 1, recordCount)
			};

			PartitionFilter filter = PartitionFilter.All();
			int count = 0;
			int max = terminateAfter;

			try
			{
				client.Query(null, stmt, filter, (key, record) =>
				{
					int rows = Interlocked.Increment(ref count);
					if (max > 0 && rows >= max)
					{
						throw new AerospikeException.QueryTerminated();
					}
				});
			}
			catch (AerospikeException.QueryTerminated)
			{
				// Expected when terminating the first pass.
			}

			Assert.AreEqual(terminateAfter, count);

			count = 0;
			max = 0;
			client.Query(null, stmt, filter, (key, record) => Interlocked.Increment(ref count));

			// Termination rolls back the last-record-key so the current record is
			// returned again when the query is resumed.
			Assert.AreEqual(recordCount - terminateAfter + 1, count);
		}
	}
}
