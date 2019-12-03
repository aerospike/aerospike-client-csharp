/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	public class TestIndex : TestSync
	{
		private const string indexName = "testindex";
		private const string binName = "testbin";

		[TestMethod]
		public void IndexCreateDrop()
		{
			Policy policy = new Policy();
			policy.SetTimeout(0);

			IndexTask task;

			// Drop index if it already exists.
			try
			{
				task = client.DropIndex(policy, args.ns, args.set, indexName);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_NOTFOUND)
				{
					throw ae;
				}
			}

			task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();

			task = client.DropIndex(policy, args.ns, args.set, indexName);
			task.Wait();

			task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();

			task = client.DropIndex(policy, args.ns, args.set, indexName);
			task.Wait();
		}
	}
}
