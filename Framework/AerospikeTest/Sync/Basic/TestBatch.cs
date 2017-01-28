/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestBatch : TestSync
	{
		private const string keyPrefix = "batchkey";
		private const string valuePrefix = "batchvalue";
		private static readonly string binName = args.GetBinName("batchbin");
		private const int size = 8;

		[ClassInitialize()]
		public static void WriteRecords(TestContext testContext)
		{
			WritePolicy policy = new WritePolicy();
			policy.expiration = 2592000;

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, valuePrefix + i);

				client.Put(policy, key, bin);
			}
		}

		[TestMethod]
		public void BatchExists()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			bool[] existsArray = client.Exists(null, keys);
			Assert.AreEqual(size, existsArray.Length);

			for (int i = 0; i < existsArray.Length; i++)
			{
				if (!existsArray[i])
				{
					Assert.Fail("Some batch records not found.");
				}
			}
		}

		[TestMethod]
		public void BatchReads()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.Get(null, keys, binName);
			Assert.AreEqual(size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				AssertBinEqual(key, record, binName, valuePrefix + (i + 1));
			}
		}

		[TestMethod]
		public void BatchReadHeaders()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.GetHeader(null, keys);
			Assert.AreEqual(size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				AssertRecordFound(key, record);
				Assert.AreNotEqual(0, record.generation);
				Assert.AreNotEqual(0, record.expiration);
			}
		}

		[TestMethod]
		public void BatchReadComplex()
		{
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
			string[] bins = new string[] {binName};
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 6), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 8), new string[] {"binnotfound"}));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			client.Get(null, records);

			AssertBatchBinEqual(records, binName, 0);
			AssertBatchBinEqual(records, binName, 1);
			AssertBatchBinEqual(records, binName, 2);
			AssertBatchRecordExists(records, binName, 3);
			AssertBatchBinEqual(records, binName, 4);
			AssertBatchBinEqual(records, binName, 5);
			AssertBatchBinEqual(records, binName, 6);

			BatchRead batch = records[7];
			AssertRecordFound(batch.key, batch.record);
			object val = batch.record.GetValue("binnotfound");
			if (val != null)
			{
				Assert.Fail("Unexpected batch bin value received");
			}

			batch = records[8];
			if (batch.record != null)
			{
				Assert.Fail("Unexpected batch record received");
			}
		}

		private void AssertBatchBinEqual(List<BatchRead> list, string binName, int i)
		{
			BatchRead batch = list[i];
			AssertBinEqual(batch.key, batch.record, binName, valuePrefix + (i + 1));
		}

		private void AssertBatchRecordExists(List<BatchRead> list, string binName, int i)
		{
			BatchRead batch = list[i];
			AssertRecordFound(batch.key, batch.record);
			Assert.AreNotEqual(0, batch.record.generation);
			Assert.AreNotEqual(0, batch.record.expiration);
		}
	}
}
