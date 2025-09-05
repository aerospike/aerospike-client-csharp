﻿/* 
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
	public class TestIndex : TestSync
	{
		private const string indexName = "testindex";
		private const string binName = "testbin";

		[TestMethod]
		public void IndexCreateDrop()
		{
			Policy policy = new();
			policy.SetTimeout(0);

			IndexTask task;

			// Drop index if it already exists.
			try
			{
				task = client.DropIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_NOTFOUND)
				{
					throw;
				}
			}

			task = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();

			task = client.DropIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName);
			task.Wait();

			// Ensure all nodes have dropped the index.
			Node[] nodes = [.. client.Nodes];

			foreach (Node node in nodes)
			{
				string cmd = IndexTask.BuildStatusCommand(SuiteHelpers.ns, indexName, node.serverVerison);
				string response = Info.Request(node, cmd);
				int code = Info.ParseResultCode(response);
				Assert.AreEqual(201, code);
			}
		}

		[TestMethod]
		public void CtxRestore()
		{
			CTX[] ctx1 =
			[
				CTX.ListIndex(-1),
				CTX.MapKey(Value.Get("key1")),
				CTX.ListValue(Value.Get(937))
			];

			string base64 = CTX.ToBase64(ctx1);
			CTX[] ctx2 = CTX.FromBase64(base64);

			Assert.AreEqual(ctx1.Length, ctx2.Length);

			for (int i = 0; i < ctx1.Length; i++)
			{
				CTX item1 = ctx1[i];
				CTX item2 = ctx2[i];

				Assert.AreEqual(item1.id, item2.id);

				object obj1 = item1.value.Object;
				object obj2 = item2.value.Object;

				if (obj1 is int v && obj2 is long v1)
				{
					// FromBase64() converts integers to long, so consider these equivalent.
					Assert.AreEqual((long)v, v1);
				}
				else
				{
					Assert.AreEqual(obj1, obj2);
				}
			}
		}
	}
}
