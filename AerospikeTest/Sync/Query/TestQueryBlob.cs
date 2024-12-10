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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryBlob : TestSync
	{
		private const string indexName = "qbindex";
		private static readonly string binName = "bb";
		private static readonly string indexNameList = "qblist";
		private static readonly string binNameList = "bblist";
		private static int size = 5;

		[ClassInitialize()]
		public static async Task Prepare(TestContext testContext)
		{
			Policy policy = new();
			policy.totalTimeout = 5000;

			try
			{
				if (!args.testAsyncAwait)
				{
					IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.BLOB);
					task.Wait();

					task = client.CreateIndex(policy, args.ns, args.set, indexNameList, binNameList, IndexType.BLOB, IndexCollectionType.LIST);
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
				var bytes = new byte[8];
				ByteUtil.LongToBytes(50000 + (ulong)i, bytes, 0);

				List<byte[]> list = new()
				{
					bytes
				};

				Key key = new Key(args.ns, args.set, i);
				Bin bin = new Bin(binName, bytes);
				Bin binList = new Bin(binNameList, list);

				if (!args.testAsyncAwait)
				{
					client.Put(null, key, bin, binList);
				}
				else
				{
					await asyncAwaitClient.Put(null, key, new[] { bin, binList }, CancellationToken.None);
				}
			}
		}

		[ClassCleanup()]
		public static void Destroy()
		{
			if (!args.testAsyncAwait)
			{
				client.DropIndex(null, args.ns, args.set, indexName);
				client.DropIndex(null, args.ns, args.set, indexNameList);
			}
			else if (args.testAsyncAwait) 
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryBlob()
		{
			var bytes = new byte[8];
			ByteUtil.LongToBytes(50003, bytes, 0);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Equal(binName, bytes));

			if (!args.testAsyncAwait) { 
				RecordSet rs = client.Query(null, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						Record record = rs.Record;
						byte[] result = (byte[])record.GetValue(binName);
						CollectionAssert.AreEqual(bytes, result);
						count++;
					}

					Assert.AreNotEqual(0, count);
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

		[TestMethod]
		public void QueryBlobInList()
		{
			var bytes = new byte[8];
			ByteUtil.LongToBytes(50003, bytes, 0);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName, binNameList);
			stmt.SetFilter(Filter.Contains(binNameList, IndexCollectionType.LIST, bytes));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(null, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						Record record = rs.Record;
						List<object> list = (List<object>)record.GetValue(binNameList);
						Assert.AreEqual(1, list.Count);

						byte[] result = (byte[])list.ElementAt(0);
						CollectionAssert.AreEqual(bytes, result);
						count++;
					}

					Assert.AreEqual(1, count);
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
