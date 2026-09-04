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
	public class TestQueryPrimary : TestSync
	{
		private const string setName = "query-primary";
		private const string valueBin = "value";
		private const string payloadBin = "payload";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			for (int i = 1; i <= 3; i++)
			{
				Key key = new(SuiteHelpers.ns, setName, "primary-" + i);
				client.Put(null, key, new Bin(valueBin, i));
			}

			byte[] largePayload = new byte[17 * 1024];
			Array.Fill(largePayload, (byte)'x');
			client.Put(null, new Key(SuiteHelpers.ns, setName, "primary-large"), new Bin(payloadBin, largePayload));
		}

		[ClassCleanup]
		public static void Destroy()
		{
			for (int i = 1; i <= 3; i++)
			{
				client.Delete(null, new Key(SuiteHelpers.ns, setName, "primary-" + i));
			}

			client.Delete(null, new Key(SuiteHelpers.ns, setName, "primary-large"));
		}

		[TestMethod]
		public void QueryPrimarySet()
		{
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = setName,
				MaxRecords = 20
			};

			int count = 0;

			using (RecordSet recordSet = client.Query(null, stmt))
			{
				while (recordSet.Next())
				{
					count++;
				}
			}

			Assert.AreEqual(4, count);
		}

		[TestMethod]
		public void QueryPrimarySetWithAction()
		{
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = setName,
				MaxRecords = 20
			};

			HashSet<string> keys = new();
			int valueCount = 0;

			Action<Key, Record> collect = (key, record) =>
			{
				keys.Add(key.userKey.ToString());
				if (record.bins.ContainsKey(valueBin))
				{
					valueCount++;
				}
			};

			client.Query(null, stmt, collect);

			Assert.AreEqual(4, keys.Count);
			Assert.AreEqual(3, valueCount, "Expected three seeded integer records plus one payload-only record.");
			Assert.IsTrue(keys.Contains("primary-large"));
		}

		[TestMethod]
		public void QueryPrimaryRecordSizeFilter()
		{
			CheckServerVersion(new Version(7, 0), "Record size filter");

			Key largeKey = new(SuiteHelpers.ns, setName, "primary-large");
			Record seeded = client.Get(null, largeKey, payloadBin);
			AssertRecordFound(largeKey, seeded);

			QueryPolicy queryPolicy = new()
			{
				filterExp = Exp.Build(Exp.GT(Exp.RecordSize(), Exp.Val(1024 * 16)))
			};

			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = setName
			};

			int count = 0;

			using (RecordSet recordSet = client.Query(queryPolicy, stmt))
			{
				while (recordSet.Next())
				{
					count++;
					Assert.IsNotNull(recordSet.Record, "Expected record payload on query result.");
					byte[] payload = recordSet.Record.GetValue(payloadBin) as byte[];
					Assert.IsNotNull(payload, "Expected payload bin on large record.");
					Assert.IsTrue(payload.Length > 1024 * 16);
				}
			}

			Assert.AreEqual(1, count, "Expected exactly one record larger than 16 KiB.");
		}
	}
}
