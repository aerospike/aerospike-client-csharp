/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	public class TestQueryExpression : TestSync
	{
		private static readonly string setName = SuiteHelpers.set + "campaign";
		private const string indexName = "exp_index";
		private const string keyPrefix = "camp";
		private const int size = 50;
		private static Expression exp = Exp.Build(Exp.Add(Exp.IntBin("campagin1"), Exp.IntBin("campagin2"), Exp.IntBin("campagin2")));

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, setName, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, exp);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			// Write records with string keys
			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, setName, keyPrefix + i);
				client.Delete(null, key); // Ensure record does not already exist.
				client.Put(null, key, new Bin("campagin1", i), new Bin("campagin2", 100), new Bin("campagin3", 100));
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, setName, indexName);
		}

		[TestMethod]
		public void QueryExpression()
		{
			int begin = 220;
			int end = 230;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(exp, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					count++;
				}
				Assert.AreEqual(11, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
