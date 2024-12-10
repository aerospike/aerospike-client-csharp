﻿/* 
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
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryContext : TestSync
	{
		private const string indexName = "listrank";
		private static readonly string binName = "list";
		private const int size = 50;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new Policy();
			policy.totalTimeout = 0; // Do not timeout on index create.

			try
			{
				if (!args.testAsyncAwait)
				{
					IndexTask task = client.CreateIndex(
					policy, args.ns, args.set, indexName, binName,
					IndexType.NUMERIC, IndexCollectionType.DEFAULT,
					CTX.ListRank(-1)
					);
					task.Wait();
				}
				else if (args.testAsyncAwait)
				{
					throw new NotImplementedException();
				}
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, i);

				List<int> list = new List<int>(5);
				list.Add(i);
				list.Add(i + 1);
				list.Add(i + 2);
				list.Add(i + 3);
				list.Add(i + 4);

				Bin bin = new Bin(binName, list);
				client.Put(null, key, bin);
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			if (!args.testAsyncAwait)
			{
				client.DropIndex(null, args.ns, args.set, indexName);
			}
			else if (args.testAsyncAwait)
			{
				throw new NotImplementedException(); 
			}
		}

		[TestMethod]
		public void QueryContext()
		{
			int begin = 14;
			int end = 18;

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, begin, end, CTX.ListRank(-1)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(null, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						Record r = rs.Record;
						IList list = r.GetList(binName);
						long received = (long)list[list.Count - 1];

						if (received < begin || received > end)
						{
							Assert.Fail("Received not between: " + begin + " and " + end);
						}
						count++;
					}
					Assert.AreEqual(5, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}
	}
}
