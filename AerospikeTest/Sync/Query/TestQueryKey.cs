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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryKey : TestSync
	{
		private const string indexName = "skindex";
		private const string keyPrefix = "skkey";
		private static readonly string binName = Suite.GetBinName("skbin");
		private const int size = 10;

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

			WritePolicy writePolicy = new()
			{
				sendKey = true
			};

			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new(binName, i);
				client.Put(writePolicy, key, bin);
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryKey()
		{
			int begin = 2;
			int end = 5;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Assert.IsNotNull(key.userKey);

					object userkey = key.userKey.Object;
					Assert.IsNotNull(userkey);
					count++;
				}
				Assert.AreEqual(4, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
