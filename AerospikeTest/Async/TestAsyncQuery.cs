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
	public class TestAsyncQuery : TestAsync
	{
		private const string indexName = "asqindex";
		private const string keyPrefix = "asqkey";
		private static readonly string binName = Suite.GetBinName("asqbin");
		private const int size = 50;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask task = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.NUMERIC);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void AsyncQuery()
		{
			WriteHandler handler = new(this);

			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new(binName, i);
				client.Put(null, handler, key, bin);
			}
			WaitTillComplete();
		}

		private class WriteHandler(TestAsyncQuery parent) : WriteListener
		{
			internal int count;

			public void OnSuccess(Key key)
			{
				int rows = Interlocked.Increment(ref count);

				if (rows == size)
				{
					int begin = 26;
					int end = 34;

					Statement stmt = new();
					stmt.SetNamespace(SuiteHelpers.ns);
					stmt.SetSetName(SuiteHelpers.set);
					stmt.SetBinNames(binName);
					stmt.SetFilter(Filter.Range(binName, begin, end));

					client.Query(null, new RecordSequenceHandler(parent), stmt);
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class RecordSequenceHandler(TestAsyncQuery parent) : RecordSequenceListener
		{
			private int count;

			public void OnRecord(Key key, Record record)
			{
				int result = record.GetInt(binName);
				parent.AssertBetween(26, 34, result);
				Interlocked.Increment(ref count);
			}

			public void OnSuccess()
			{
				parent.AssertEquals(9, count);
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
