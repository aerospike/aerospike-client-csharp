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
	public class TestAsyncScanPartitions : TestAsync
	{
		private const string KeyPrefix = "tierA-asp-";
		private const string BinName = "aspbin";
		private const int RecordCount = 10;

		[ClassInitialize]
		public static void SeedRecords(TestContext testContext)
		{
			AsyncMonitor monitor = new();
			for (int i = 1; i <= RecordCount; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, KeyPrefix + i);
				Bin bin = new(BinName, i);
				client.Put(null, new SeedWriteHandler(monitor), key, bin);
			}
			monitor.WaitTillComplete();
		}

		[TestMethod]
		public void AsyncScanPartitions()
		{
			client.ScanPartitions(null, new ScanPartitionHandler(this), PartitionFilter.All(),
				SuiteHelpers.ns, SuiteHelpers.set, BinName);
			WaitTillComplete();
		}

		private static bool IsSeededRecord(Record record)
		{
			return record.bins.ContainsKey(BinName);
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

		private class ScanPartitionHandler(TestAsyncScanPartitions parent) : RecordSequenceListener
		{
			private int count;
			private int valueSum;

			public void OnRecord(Key key, Record record)
			{
				if (!IsSeededRecord(record))
				{
					return;
				}

				int value = record.GetInt(BinName);
				parent.AssertBetween(1, RecordCount, value);
				Interlocked.Increment(ref count);
				Interlocked.Add(ref valueSum, value);
			}

			public void OnSuccess()
			{
				if (!parent.AssertEquals(RecordCount, count))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.AssertEquals(55, valueSum); // 1+2+...+10
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
