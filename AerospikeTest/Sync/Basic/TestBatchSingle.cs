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
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestBatchSingle : TestSync
	{
		private const string BinName = "bsbin";
		private const string BinValue = "batch-single-value";
		private static Key seedKey;

		[ClassInitialize]
		public static void Initialize(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();

			seedKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "batch-single-seed");
			client.Put(null, seedKey, new Bin(BinName, BinValue));
		}

		[TestMethod]
		public void BatchSingleExists()
		{
			Key[] keys = [seedKey];

			bool[] exists = client.Exists(null, keys);

			Assert.AreEqual(1, exists.Length);
			Assert.IsTrue(exists[0]);
		}

		[TestMethod]
		public void BatchSingleGetHeader()
		{
			Key[] keys = [seedKey];

			Record[] records = client.GetHeader(null, keys);

			Assert.AreEqual(1, records.Length);
			AssertRecordFound(seedKey, records[0]);
			Assert.IsTrue(records[0].generation > 0);
		}

		[TestMethod]
		public void BatchSingleOperateRead()
		{
			List<BatchRecord> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			bool status = client.Operate(null, records);

			Assert.IsTrue(status);
			Assert.AreEqual(0, records[0].resultCode);
			AssertBinEqual(seedKey, records[0].record, BinName, BinValue);
		}

		[TestMethod]
		public void BatchSingleUDF()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "batch-single-udf");
			Key[] keys = [key];

			client.Delete(null, null, keys);

			BatchResults results = client.Execute(null, null, keys, "record_example", "writeBin",
				Value.Get(BinName), Value.Get("udf-single"));

			Assert.IsTrue(results.status);
			Assert.AreEqual(1, results.records.Length);
			Assert.AreEqual(0, results.records[0].resultCode);

			Record record = client.Get(null, key, BinName);
			AssertRecordFound(key, record);
			Assert.AreEqual("udf-single", record.GetString(BinName));
		}
	}
}
