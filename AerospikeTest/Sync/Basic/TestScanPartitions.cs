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
	public class TestScanPartitions : TestSync
	{
		private const string KeyPrefix = "tierA-sp-";
		private const string BinName = "spbin";
		private const int RecordCount = 12;

		[ClassInitialize]
		public static void SeedRecords(TestContext testContext)
		{
			for (int i = 1; i <= RecordCount; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, KeyPrefix + i);
				client.Put(null, key, new Bin(BinName, i));
			}
		}

		[TestMethod]
		public void ScanPartitionsFindsSeededRecords()
		{
			int count = 0;
			int valueSum = 0;

			client.ScanPartitions(null, PartitionFilter.All(), SuiteHelpers.ns, SuiteHelpers.set,
				(key, record) =>
				{
					if (!IsSeededRecord(record))
					{
						return;
					}

					count++;
					valueSum += record.GetInt(BinName);
				},
				BinName);

			Assert.AreEqual(RecordCount, count);
			Assert.AreEqual(78, valueSum); // 1+2+...+12
		}

		private static bool IsSeededRecord(Record record)
		{
			return record.bins.ContainsKey(BinName);
		}
	}
}
