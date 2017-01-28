/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Test
{
	public class BatchInit : TestSync
	{
		public BatchInit()
		{
			WritePolicy policy = new WritePolicy();
			policy.expiration = 2592000;

			for (int i = 1; i <= TestBatch.size; i++)
			{
				Key key = new Key(args.ns, args.set, TestBatch.keyPrefix + i);
				Bin bin = new Bin(TestBatch.binName, TestBatch.valuePrefix + i);

				client.Put(policy, key, bin);
			}
		}
	}

	public class TestBatch : TestSync, Xunit.IClassFixture<BatchInit>
	{
		public const string keyPrefix = "batchkey";
		public const string valuePrefix = "batchvalue";
		public static readonly string binName = args.GetBinName("batchbin");
		public const int size = 8;

		BatchInit fixture;

		public TestBatch(BatchInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void BatchExists()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			bool[] existsArray = client.Exists(null, keys);
			Xunit.Assert.Equal(size, existsArray.Length);

			for (int i = 0; i < existsArray.Length; i++)
			{
				if (!existsArray[i])
				{
					Fail("Some batch records not found.");
				}
			}
		}

		[Xunit.Fact]
		public void BatchReads()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.Get(null, keys, binName);
			Xunit.Assert.Equal(size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				AssertBinEqual(key, record, binName, valuePrefix + (i + 1));
			}
		}

		[Xunit.Fact]
		public void BatchReadHeaders()
		{
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.GetHeader(null, keys);
			Xunit.Assert.Equal(size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				AssertRecordFound(key, record);
				Xunit.Assert.NotEqual(0, record.generation);
				Xunit.Assert.NotEqual(0, record.expiration);
			}
		}

		[Xunit.Fact]
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
				Fail("Unexpected batch bin value received");
			}

			batch = records[8];
			if (batch.record != null)
			{
				Fail("Unexpected batch record received");
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
			Xunit.Assert.NotEqual(0, batch.record.generation);
			Xunit.Assert.NotEqual(0, batch.record.expiration);
		}
	}
}
