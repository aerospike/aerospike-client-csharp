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
	public class TestQuerySendKey : TestSync
	{
		private const string indexName = "qskindex";
		private const string keyPrefix = "qskkey";
		private static readonly string binName = "qskbin";
		private const int size = 5;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.INTEGER);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			// Write without sendKey so the server stores only the digest.
			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new(binName, i);
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryKeyThenReadWithSendKey()
		{
			// SUP-154 / CLIENT-5595: SI query returns digest-only keys (userKey null).
			// A subsequent read with sendKey=true must succeed, not NullReferenceException.
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Equal(binName, 3));

			Key queryKey = null;
			RecordSet rs = client.Query(null, stmt);

			try
			{
				Assert.IsTrue(rs.Next());
				queryKey = rs.Key;
				Assert.IsNull(queryKey.userKey);
				Assert.IsNotNull(queryKey.digest);
			}
			finally
			{
				rs.Close();
			}

			Policy readPolicy = new()
			{
				sendKey = true
			};

			Record record = client.Get(readPolicy, queryKey, binName);
			AssertRecordFound(queryKey, record);
			Assert.AreEqual(3, record.GetInt(binName));
		}

		[TestMethod]
		public void QueryKeyThenBatchReadWithSendKey()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, 1, 2));

			List<Key> queryKeys = [];
			RecordSet rs = client.Query(null, stmt);

			try
			{
				while (rs.Next())
				{
					Key key = rs.Key;
					Assert.IsNull(key.userKey);
					queryKeys.Add(key);
				}
			}
			finally
			{
				rs.Close();
			}

			Assert.AreEqual(2, queryKeys.Count);

			BatchPolicy batchPolicy = new()
			{
				sendKey = true
			};

			Record[] records = client.Get(batchPolicy, [.. queryKeys], binName);

			Assert.AreEqual(2, records.Length);
			foreach (Record record in records)
			{
				Assert.IsNotNull(record);
				Assert.IsTrue(record.GetInt(binName) is 1 or 2);
			}
		}
	}
}
