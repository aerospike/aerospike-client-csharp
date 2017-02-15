/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryFilter : TestSync
	{
		private const string indexName = "profileindex";
		private const string keyPrefix = "profilekey";
		private static readonly string binName = args.GetBinName("name");

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask rtask = client.Register(null, assembly, "Aerospike.Test.Resources.filter_example.lua", "filter_example.lua", Language.LUA);
			rtask.Wait();

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask itask = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING);
			itask.Wait();

			WriteRecord(keyPrefix + 1, "Charlie", "cpass");
			WriteRecord(keyPrefix + 2, "Bill", "hknfpkj");
			WriteRecord(keyPrefix + 3, "Doug", "dj6554");
		}

		private static void WriteRecord(string userKey, string name, string password)
		{
			Key key = new Key(args.ns, args.set, userKey);
			Bin bin1 = new Bin("name", name);
			Bin bin2 = new Bin("password", password);
			client.Put(null, key, bin1, bin2);
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QueryFilter()
		{
			string nameFilter = "Bill";
			string passFilter = "hknfpkj";

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilter(Filter.Equal(binName, nameFilter));
			stmt.SetAggregateFunction(Assembly.GetExecutingAssembly(), "Aerospike.Test.Resources.filter_example.lua", "filter_example", "profile_filter", Value.Get(passFilter));

			// passFilter will be applied in filter_example.lua.
			ResultSet rs = client.QueryAggregate(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					IDictionary map = (IDictionary)rs.Object;
					Assert.AreEqual(nameFilter, map["name"]);
					Assert.AreEqual(passFilter, map["password"]);
					count++;
				}
				Assert.AreNotEqual(0, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
