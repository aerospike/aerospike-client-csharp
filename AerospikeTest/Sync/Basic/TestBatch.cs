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
using Neo.IronLua;
using System;
using System.Collections;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Security.Policy;
using System.Text;

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

			string[] bins = new string[] { BinName };
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 6), ops));
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, KeyPrefix + 8), new string[] { "binnotfound" }));

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
		public void BatchReadMax()
		{
			Key[] keys = new Key[5001];
			BatchRecord[] batchRecords = new BatchRecord[5001];
			for (int i = 0; i < 5001; i++)
			{
				keys[i] = new Key(args.ns, args.set, i);
				batchRecords[i] = new BatchRead(keys[i], true);
			}

			var records = client.Get(null, keys);

			var result = client.Operate(null, null, keys, Operation.Get());

			var status = client.Operate(null, batchRecords.ToList());
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

		[TestMethod]
		public void BatchParamError()
		{
			List<Key> keys = new();
			List<Object> list_bin = new()
			{
				0,
				"Hello",
				Encoding.ASCII.GetBytes("World"),
				true
			};

			for (int i=0; i<8; i++) 
			{
				Key key = new Key(args.ns, args.set, i);
				keys.Add(key);

				List<Object> newList = new(list_bin);
				newList.Add(i);
				Bin bin = new Bin("list_bin", newList);

				client.Put(null, key, bin);
			}

			BatchPolicy bp = new BatchPolicy();
			// the Exp.val(3) is invalid parameter in this expression
			// Instead use ArrayList<Integer>(){{add(3);}} would work correctly
			Expression expr = Exp.Build(Exp.EQ(ListExp.GetByValueRange(ListReturnType.VALUE, Exp.Val(3), Exp.Val(5), Exp.Bin("list_bin", Exp.Type.LIST)), Exp.Val(3)));
			bp.filterExp = expr;
			bp.failOnFilteredOut = true;
			bp.totalTimeout = 0;
			bp.socketTimeout = 0;

			Record[] records = client.Get(bp, keys.ToArray());
		}

		[TestMethod]
		public void BatchGetInconsistent()
		{
			var keyList = new Key[9];

			for (int i = 0; i < 9; i++)
			{
				keyList[i] = new Key(args.ns, args.set, i);
				client.Delete(null, keyList[i]);
			}

			for (int i = 0; i < 8; i++)
			{
				client.Put(null, keyList[i], new Bin("bin", 1));
			}
			client.Put(null, keyList[8], new Bin("bin", 10));

			Policy policy = new()
			{
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin("bin"), Exp.Val(1))),
				failOnFilteredOut = false
			};
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			for (int i = 0; i < 9; i++)
			{
				var record = client.Get(policy, keyList[i]);
				Console.WriteLine(record);
			}

			BatchPolicy batchPolicy = new()
			{
				filterExp = policy.filterExp,
				failOnFilteredOut = false
			};
			if (args.testProxy)
			{
				batchPolicy.totalTimeout = args.proxyTotalTimeout;
			}

			var result = client.Get(batchPolicy, keyList);
			Console.WriteLine(result);

			keyList[8] = new Key("invalid", args.set, 8);

			var result2 = client.Get(batchPolicy, keyList);
			Console.WriteLine(result2);
		}

		[TestMethod]
		public void Batch5001()
		{
			var keyList = new Key[5001];
			var recordList = new BatchRecord[5001];

			for (int i = 0; i < 5001; i++)
			{
				keyList[i] = new Key(args.ns, args.set, i);
				client.Delete(null, keyList[i]);
				recordList[i] = new BatchRead(keyList[i], true);
			}

			Policy policy = new();
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			var result = client.Get(null, keyList);
			Console.WriteLine(result);

			var result2 = client.Operate(null, null, keyList, Operation.Get());
			Console.WriteLine(result2);

			var result3 = client.Operate(null, recordList.ToList());
			Console.WriteLine(result3);
		}

		[TestMethod]
		public void InDoubtBatch()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.test_ops.lua", "test_ops.lua", Language.LUA);
			task.Wait();

			var recordList = new BatchRecord[100];

			for (int i = 0; i < 100; i++)
			{
				var key = new Key(args.ns, args.set, i);
				client.Delete(null, key);
				client.Put(null, key, new Bin("bin", 1));
				recordList[i] = new BatchRead(key, true);
				Dictionary<string, int> bin = new()
				{
					{ "bin", i }
				};
				recordList[i] = new BatchUDF(null, key, "test_ops", "wait_and_update",
					new Value[]
					{
						Value.Get(bin),
						Value.Get(2)
					}
				);
			}

			BatchPolicy policy = new();
			if (args.testProxy)
			{
				policy.totalTimeout = 10000;
				policy.socketTimeout = 1000;
				policy.maxRetries = 5;
			}

			var result = client.Operate(policy, recordList.ToList());
			Console.WriteLine(result);
		}

		[TestMethod]
		public void BigWriteBlock()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.test_ops.lua", "test_ops.lua", Language.LUA);
			task.Wait();

			var recordList = new BatchRecord[3];
			var writeBlockSize = 1048576;
			
			Dictionary<string, string> bigBin = new()
			{
				{ "bigbin", new string('a', writeBlockSize) }
			};
			Dictionary<string, string> smallBin = new()
			{
				{ "bigbin", new string('a', 1000) }
			};

			Key key1 = new(args.ns, args.set, 1);
			Key key2 = new(args.ns, args.set, 2);
			Key key3 = new(args.ns, args.set, 3);

			recordList[0] = new BatchUDF(null, key1, "test_ops", "rec_create", new Value[] { Value.Get(bigBin) });
			recordList[1] = new BatchUDF(null, key2, "test_ops", "rec_create", new Value[] { Value.Get(bigBin) });
			recordList[2] = new BatchUDF(null, key3, "test_ops", "rec_create", new Value[] { Value.Get(smallBin) });

			var result = client.Operate(null, recordList.ToList());
			Console.WriteLine(result);
		}

		[TestMethod]
		public void BatchGetRecordResultCode()
		{
			var keys = new Key[6];
			var mod = 5;

			for (int i = 0; i < keys.Length; i++)
			{
				keys[i] = new Key(args.ns, args.set, i);
				byte[] bytes = null;

				if (i % mod == 0)
				{
					bytes = new byte[1] { (byte)mod };
				}
				else
				{
					var length = i % mod + 1;
					bytes = new byte[length];
					for (int j = 0; j < length; j++)
					{
						bytes[j] = (byte)(j % mod + 1);
					}
				}

				client.Put(null, keys[i], new Bin("bin_b", bytes));
			}

			var expr = Exp.Build(BitExp.Add(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(255), false, BitOverflowAction.FAIL, Exp.BlobBin("bin_b")));
			var policy = new BatchPolicy
			{
				filterExp = expr
			};
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			client.Get(policy, keys);
		}

		[TestMethod]
		public void BatchGetFailureProxy()
		{
			var keyList = new Key[10];

			for (int i = 0; i < 10; i++)
			{
				keyList[i] = new Key(args.ns, args.set, i);
				client.Delete(null, keyList[i]);
			}

			for (int i = 0; i < 10; i++)
			{
				var values = new Value[]
				{
					new Value.NullValue(),
					Value.Get(0),
					Value.Get(10 + i),
					Value.Get("string_test0"),
					Value.Get(new int[] {26, 27, 28, 0}),
					Value.GetAsGeoJSON("{\"type\": \"Polygon\", \"coordinates\": [[[-122.5, 37.0], [-121.0, 37.0], [-121.0, 38.08], [-122.5, 38.08], [-122.5, 37.0]]]}")
				};
				client.Put(null, keyList[i], new Bin("list_bin", values));
			}

			BatchPolicy policy = new()
			{
				filterExp = Exp.Build(Exp.EQ(ListExp.GetByValueRange(ListReturnType.VALUE, Exp.Val(10), Exp.Val(13), Exp.ListBin("list_bin")), Exp.Val(11)))
			};
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			var result = client.Get(policy, keyList);
		}

		[TestMethod]
		public void ZeroBatchIndexThreads()
		{
			List<BatchRecord> brs = new();
			for (int i = 0; i < 100; i++)
			{
				Key key = new Key(args.ns, args.set, i);
				Operation[] ops = new Operation[] {
					Operation.Put(new Bin("bin", i))
				};
				brs.Add(new BatchWrite(key, ops));
			}

			BatchPolicy bp = new BatchPolicy();
			bp.totalTimeout = 10000;

			try
			{
				var isDone = client.Operate(bp, brs);
			}
			catch (Exception ex)
			{

			}
		}

		[TestMethod]
		public void BatchTotalTimeout()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.test_ops.lua", "test_ops.lua", Language.LUA);
			task.Wait();

			Key key = new Key(args.ns, args.set, "to");
			client.Delete(null, key);
			WritePolicy wp = new()
			{
				totalTimeout = 10000,
				socketTimeout = 1000,
				maxRetries = 5
			};
			Dictionary<string, int> bin = new()
			{
				{ "bin", 100 }
			};

			//try
			//{
			Object res = client.Execute(wp, key, "test_ops", "wait_and_create", new Value[] {
					Value.Get(bin),
					Value.Get(2),
				});
			//}
			//catch (Exception e)
			//{
			//}
		}

		[TestMethod]
		public void BatchTotalTimeout2()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.test_ops.lua", "test_ops.lua", Language.LUA);
			task.Wait();

			List<BatchRecord> batchRecords = new();
			for (int i = 0; i < 1; i++)
			{
				Key key = new Key(args.ns, args.set, i);
				Dictionary<string, int> bin = new()
				{
					{ "bin", 100 }
				};
				batchRecords.Add(
						new BatchUDF(null, key, "test_ops", "wait_and_create", new Value[] {
					Value.Get(bin),
					Value.Get(2),
				}));
			}
			BatchPolicy bp = new() {
				totalTimeout = 2000,
				socketTimeout = 1000,
				maxRetries = 5
			};
			//try
			//{
				bool result = client.Operate(bp, batchRecords);
			//}
		}

		[TestMethod]
		public void BatchPolicyPrecedence()
		{
			var key = new Key(args.ns, args.set, 11111);
			Bin[] bins =
			{
				new Bin("age", 10),
				new Bin("count", 0),
				new Bin("list", new int[] { 0, 1, 2, 3 })
			};
			client.Put(null, key, bins);

			var batchPolicy = new BatchPolicy
			{
				filterExp = Exp.Build(Exp.EQ(ListExp.GetByRank(ListReturnType.RANK, Exp.Type.INT, Exp.Val(0), Exp.ListBin("list")), Exp.Val(1)))
			};

			var batchDeletePolicy = new BatchDeletePolicy
			{
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin("count"), Exp.Val(0)))
			};

			var result = client.Delete(batchPolicy, batchDeletePolicy, new Key[] { key });
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
