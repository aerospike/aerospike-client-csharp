/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	public class TestQueryInteger : TestSync
	{
		private const string indexName = "queryindexint";
		private const string keyPrefix = "querykeyint";
		private static readonly string binName = args.GetBinName("querybinint");
		private const int size = 50;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			client.DropIndex(null, args.ns, args.set, indexName);
		}

		[TestMethod]
		public void QueryInteger()
		{
			int begin = 14;
			int end = 18;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Range(binName, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					count++;
				}
				Assert.AreEqual(5, count);
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
