/*
 * Copyright 2012-2026 Aerospike, Inc.
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
	public class TestAsyncQueryPartitions : TestAsync
	{
		private const string indexName = "aqpindex";
		private const string keyPrefix = "aqpkey";
		private static readonly string binName = "aqpbin";
		private const int size = 30;

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0
			};

			try
			{
				IndexTask task = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.INTEGER);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			AsyncMonitor monitor = new();
			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Bin bin = new(binName, i);
				client.Put(null, new SeedWriteHandler(monitor), key, bin);
			}
			monitor.WaitTillComplete();
		}

		[ClassCleanup]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void AsyncQueryPartitions()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetBinNames(binName);
			stmt.SetFilter(Filter.Range(binName, 10, 20));

			client.QueryPartitions(null, new PartitionQueryHandler(this), stmt, PartitionFilter.All());
			WaitTillComplete();
		}

		private class SeedWriteHandler(AsyncMonitor monitor) : WriteListener
		{
			public void OnSuccess(Key key)
			{
				monitor.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				monitor.SetError(e);
				monitor.NotifyCompleted();
			}
		}

		private class PartitionQueryHandler(TestAsyncQueryPartitions parent) : RecordSequenceListener
		{
			private int count;

			public void OnRecord(Key key, Record record)
			{
				int value = record.GetInt(binName);
				parent.AssertBetween(10, 20, value);
				Interlocked.Increment(ref count);
			}

			public void OnSuccess()
			{
				parent.AssertEquals(11, count);
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
