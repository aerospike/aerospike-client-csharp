/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryPage : TestSync
	{
		private const string keyPrefix = "pagekey";
		private static readonly string binName = Suite.GetBinName("name");
		private static readonly string indexName = "pqidx";

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.NUMERIC);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}
			WriteRecords(binName, 190);
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryPage()
		{
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = SuiteHelpers.set,
				BinNames = new string[] { binName },
				Filter = Filter.Range(binName, 1, 200),
				MaxRecords = 100
			};

			PartitionFilter filter = PartitionFilter.All();

			int totalRecords = 0;

			// Query 3 pages of records.
			for (int i = 0; i < 3 && !filter.Done; i++)
			{
				RecordSet rs = client.QueryPartitions(null, stmt, filter);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}

					totalRecords += count;
				}
				finally
				{
					rs.Close();
				}
			}

			Assert.AreEqual(190, totalRecords);
		}

		private static void WriteRecords(string binName, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new(binName, i);
				client.Put(null, key, bin);
			}
		}
	}
}
