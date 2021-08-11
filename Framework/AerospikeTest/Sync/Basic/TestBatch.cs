/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestBatch : TestSync
	{
		private const string BinName = "batchbin";
		private const string ListBin = "listbin";
		private const string KeyPrefix = "tbatkey";
		private const string ValuePrefix = "batchvalue";
		private const int Size = 8;

		[ClassInitialize()]
		public static void WriteRecords(TestContext testContext)
		{
			WritePolicy policy = new WritePolicy();
			policy.expiration = 2592000;

			for (int i = 1; i <= Size; i++)
			{
				Key key = new Key(args.ns, args.set, KeyPrefix + i);
				Bin bin = new Bin(BinName, ValuePrefix + i);

				List<int> list = new List<int>();

				for (int j = 0; j < i; j++)
				{
					list.Add(j * i);
				}

				Bin listBin = new Bin(ListBin, list);

				if (i != 6)
				{
					client.Put(policy, key, bin, listBin);
				}
				else
				{
					client.Put(policy, key, new Bin(BinName, i), listBin);
				}
			}
		}

		[TestMethod]
		public void BatchExists()
		{
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			bool[] existsArray = client.Exists(null, keys);
			Assert.AreEqual(Size, existsArray.Length);

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
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			Record[] records = client.Get(null, keys, BinName);
			Assert.AreEqual(Size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				if (i != 5)
				{
					AssertBinEqual(key, record, BinName, ValuePrefix + (i + 1));
				}
				else
				{
					AssertBinEqual(key, record, BinName, i + 1);
				}
			}
		}

		[TestMethod]
		public void BatchReadHeaders()
		{
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			Record[] records = client.GetHeader(null, keys);
			Assert.AreEqual(Size, records.Length);

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

			// bin * 8
			Expression exp = Exp.Build(Exp.Mul(Exp.IntBin(BinName), Exp.Val(8)));
			Operation[] ops = Operation.Array(ExpOperation.Read(BinName, exp, ExpReadFlags.DEFAULT));

			string[] bins = new string[] {BinName};
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 6), ops));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 8), new string[] {"binnotfound"}));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			client.Get(null, records);

			AssertBatchBinEqual(records, BinName, 0);
			AssertBatchBinEqual(records, BinName, 1);
			AssertBatchBinEqual(records, BinName, 2);
			AssertBatchRecordExists(records, BinName, 3);
			AssertBatchBinEqual(records, BinName, 4);

			BatchRead batch = records[5];
			AssertRecordFound(batch.key, batch.record);
			int v = batch.record.GetInt(BinName);
			Assert.AreEqual(48, v);

			AssertBatchBinEqual(records, BinName, 6);

			batch = records[7];
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

		[TestMethod]
		public void BatchListOperate()
		{
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			Record[] records = client.Get(null, keys,
				ListOperation.Size(ListBin),
				ListOperation.GetByIndex(ListBin, -1, ListReturnType.VALUE));

			Assert.AreEqual(Size, records.Length);

			for (int i = 0; i < records.Length; i++)
			{
				Record record = records[i];
				IList results = record.GetList(ListBin);
				long size = (long)results[0];
				long val = (long)results[1];

				Assert.AreEqual(i + 1, size);
				Assert.AreEqual(i * (i + 1), val);
			}
		}

		private void AssertBatchBinEqual(List<BatchRead> list, string binName, int i)
		{
			BatchRead batch = list[i];
			AssertBinEqual(batch.key, batch.record, binName, ValuePrefix + (i + 1));
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
