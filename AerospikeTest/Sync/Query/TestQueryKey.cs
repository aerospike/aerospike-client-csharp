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
using Aerospike.Client.Proxy;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryKey : TestSync
	{
		private const string indexName = "skindex";
		private const string keyPrefix = "skkey";
		private static readonly string binName = args.GetBinName("skbin");
		private const int size = 10;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				if (!args.testProxy || (args.testProxy && nativeClient != null))
				{
					IndexTask itask = nativeClient.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
					itask.Wait();
				}
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			WritePolicy writePolicy = new WritePolicy();
			writePolicy.sendKey = true;
			if (args.testProxy)
			{
				writePolicy.totalTimeout = args.proxyTotalTimeout;
			}

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);
				client.Put(writePolicy, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			if (!args.testProxy || (args.testProxy && nativeClient != null))
			{
				nativeClient.DropIndex(null, args.ns, args.set, indexName);
			}
		}

		[TestMethod]
		public void QueryKey()
		{
			int begin = 2;
			int end = 5;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
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

		[TestMethod]
		public void QueryException()
		{
			for (int i = 0; i < 5; i++)
			{
				Key k = new Key(args.ns, args.set, i);
				client.Put(null, k, new Bin("bin", i));
			}
			Statement stmt = new();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			Expression exp = Exp.Build(Exp.Or(Exp.EQ(Exp.Val(1), Exp.Val(1)), Exp.Val("bad expression")));
			QueryPolicy qp = new()
			{
				filterExp = exp
			};
			try
			{
				RecordSet rcs = client.Query(qp, stmt);
				while (rcs.Next())
				{
					System.Console.WriteLine(rcs.Record);
				}
				rcs.Close();
			}
			catch (System.Exception e)
			{
				System.Console.WriteLine(e);
			}
		}
	}
}
