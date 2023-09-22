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
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;
using System.Security.Policy;

namespace Aerospike.Test
{
	[TestClass]
	public class TestBatch : TestSync
	{
		private const string BinName = "bbin";
		private const string BinName2 = "bbin2";
		private const string BinName3 = "bbin3";
		private const string ListBin = "lbin";
		private const string ListBin2 = "lbin2";
		private const string KeyPrefix = "tbatkey";
		private const string ValuePrefix = "batchvalue";
		private const int Size = 8;

		[ClassInitialize()]
		public static void WriteRecords(TestContext testContext)
		{
			WritePolicy policy = new WritePolicy();
			policy.expiration = 2592000;
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			for (int i = 1; i <= Size; i++)
			{
				Key key = new Key(args.ns, args.set, KeyPrefix + i);
				Bin bin = new Bin(BinName, ValuePrefix + i);

				List<int> list = new List<int>();

				for (int j = 0; j < i; j++)
				{
					list.Add(j * i);
				}

				List<int> list2 = new List<int>();

				for (int j = 0; j < 2; j++)
				{
					list2.Add(j);
				}

				Bin listBin = new Bin(ListBin, list);
				Bin listBin2 = new Bin(ListBin2, list2);

				if (i != 6)
				{
					client.Put(policy, key, bin, listBin, listBin2);
				}
				else
				{
					client.Put(policy, key, new Bin(BinName, i), listBin, listBin2);
				}
			}

			// Add records that will eventually be deleted.
			client.Put(policy, new Key(args.ns, args.set, 10000), new Bin(BinName, 10000));
			client.Put(policy, new Key(args.ns, args.set, 10001), new Bin(BinName, 10001));
			client.Put(policy, new Key(args.ns, args.set, 10002), new Bin(BinName, 10002));
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
				// ttl can be zero if server default-ttl = 0.
				//Assert.AreNotEqual(0, record.expiration);
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
		public void BatchListReadOperate()
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

		[TestMethod]
		public void BatchListWriteOperate()
		{
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			// Add integer to list and get size and last element of list bin for all records.
			BatchResults bresults = client.Operate(null, null, keys,
				ListOperation.Insert(ListBin2, 0, Value.Get(1000)),
				ListOperation.Size(ListBin2),
				ListOperation.GetByIndex(ListBin2, -1, ListReturnType.VALUE)
				);

			for (int i = 0; i < bresults.records.Length; i++)
			{
				BatchRecord br = bresults.records[i];
				Assert.AreEqual(0, br.resultCode);

				IList results = br.record.GetList(ListBin2);
				long size = (long)results[1];
				long val = (long)results[2];

				Assert.AreEqual(3, size);
				Assert.AreEqual(1, val);
			}
		}

		[TestMethod]
		public void BatchReadAllBins()
		{
			Key[] keys = new Key[Size];
			for (int i = 0; i < Size; i++)
			{
				keys[i] = new Key(args.ns, args.set, KeyPrefix + (i + 1));
			}

			Bin bin = new Bin("bin5", "NewValue");

			BatchResults bresults = client.Operate(null, null, keys,
				Operation.Put(bin),
				Operation.Get()
				);

			for (int i = 0; i < bresults.records.Length; i++)
			{
				BatchRecord br = bresults.records[i];
				Assert.AreEqual(0, br.resultCode);

				Record r = br.record;

				string s = r.GetString(bin.name);
				Assert.AreEqual("NewValue", s);

				object obj = r.GetValue(BinName);
				Assert.IsNotNull(obj);
			}
		}

		[TestMethod]
		public void BatchWriteComplex()
		{
			Expression wexp1 = Exp.Build(Exp.Add(Exp.IntBin(BinName), Exp.Val(1000)));

			Operation[] wops1 = Operation.Array(Operation.Put(new Bin(BinName2, 100)));
			Operation[] wops2 = Operation.Array(ExpOperation.Write(BinName3, wexp1, ExpWriteFlags.DEFAULT));
			Operation[] rops1 = Operation.Array(Operation.Get(BinName2));
			Operation[] rops2 = Operation.Array(Operation.Get(BinName3));

			BatchWritePolicy wp = new BatchWritePolicy();
			wp.sendKey = true;

			BatchWrite bw1 = new BatchWrite(new Key(args.ns, args.set, KeyPrefix + 1), wops1);
			BatchWrite bw2 = new BatchWrite(new Key("invalid", args.set, KeyPrefix + 1), wops1);
			BatchWrite bw3 = new BatchWrite(wp, new Key(args.ns, args.set, KeyPrefix + 6), wops2);
			BatchDelete bd1 = new BatchDelete(new Key(args.ns, args.set, 10002));

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(bw1);
			records.Add(bw2);
			records.Add(bw3);
			records.Add(bd1);

			bool status = client.Operate(null, records);

			Assert.IsFalse(status); // "invalid" namespace triggers the false status.
			Assert.AreEqual(0, bw1.resultCode);
			AssertBinEqual(bw1.key, bw1.record, BinName2, 0);
			Assert.AreEqual(ResultCode.INVALID_NAMESPACE, bw2.resultCode);
			Assert.AreEqual(0, bw3.resultCode);
			AssertBinEqual(bw3.key, bw3.record, BinName3, 0);
			Assert.AreEqual(ResultCode.OK, bd1.resultCode);

			BatchRead br1 = new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), rops1);
			BatchRead br2 = new BatchRead(new Key(args.ns, args.set, KeyPrefix + 6), rops2);
			BatchRead br3 = new BatchRead(new Key(args.ns, args.set, 10002), true);

			records.Clear();
			records.Add(br1);
			records.Add(br2);
			records.Add(br3);

			status = client.Operate(null, records);

			Assert.IsFalse(status); // Read of deleted record causes status to be false.
			AssertBinEqual(br1.key, br1.record, BinName2, 100);
			AssertBinEqual(br2.key, br2.record, BinName3, 1006);
			Assert.AreEqual(ResultCode.KEY_NOT_FOUND_ERROR, br3.resultCode);
		}

		[TestMethod]
		public void BatchDelete()
		{
			// Define keys
			Key[] keys = new Key[] { new Key(args.ns, args.set, 10000), new Key(args.ns, args.set, 10001) };

			// Ensure keys exists
			bool[] exists = client.Exists(null, keys);
			Assert.IsTrue(exists[0]);
			Assert.IsTrue(exists[1]);

			// Delete keys
			BatchResults br = client.Delete(null, null, keys);
			Assert.IsTrue(br.status);

			// Ensure keys do not exist
			exists = client.Exists(null, keys);
			Assert.IsFalse(exists[0]);
			Assert.IsFalse(exists[1]);
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
            // ttl can be zero if server default-ttl = 0.
            // Assert.AreNotEqual(0, batch.record.expiration);
		}
	}
}
