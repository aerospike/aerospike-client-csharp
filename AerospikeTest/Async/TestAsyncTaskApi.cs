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
	public class TestAsyncTaskApi : TestAsync
	{
		private static readonly CancellationTokenSource tokenSource = new();
		private const string BinName = "taskbin";
		private const string BinValue = "task-value";
		private static Key seedKey;

		[ClassInitialize]
		public static void Initialize(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();

			seedKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "async-task-seed");
			client.Put(null, tokenSource.Token, seedKey, new Bin(BinName, BinValue)).Wait();
		}

		[TestMethod]
		public void AsyncDeleteWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-task-delete");
			client.Put(null, tokenSource.Token, key, new Bin(BinName, BinValue)).Wait();

			bool deleted = client.Delete(null, tokenSource.Token, key).Result;
			Assert.IsTrue(deleted);

			bool exists = client.Exists(null, tokenSource.Token, key).Result;
			Assert.IsFalse(exists);
		}

		[TestMethod]
		public void AsyncTouchedWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-task-touched-missing");

			bool exists = client.Touched(null, tokenSource.Token, key).Result;
			Assert.IsFalse(exists);
		}

		[TestMethod]
		public void AsyncOperateWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-task-operate");
			client.Delete(null, tokenSource.Token, key).Wait();

			Record record = client.Operate(null, tokenSource.Token, key,
				Operation.Put(new Bin(BinName, BinValue)),
				Operation.Get(BinName)).Result;

			Assert.IsNotNull(record);
			Assert.AreEqual(BinValue, record.GetString(BinName));
		}

		[TestMethod]
		public void AsyncBatchGetWithTask()
		{
			Key[] keys = [seedKey];

			Record[] records = client.Get(null, tokenSource.Token, keys, BinName).Result;

			Assert.AreEqual(1, records.Length);
			AssertRecordFound(seedKey, records[0]);
			Assert.AreEqual(BinValue, records[0].GetString(BinName));
		}

		[TestMethod]
		public void AsyncBatchExistsWithTask()
		{
			Key[] keys = [seedKey];

			bool[] exists = client.Exists(null, tokenSource.Token, keys).Result;

			Assert.AreEqual(1, exists.Length);
			Assert.IsTrue(exists[0]);
		}

		[TestMethod]
		public void AsyncBatchGetHeaderWithTask()
		{
			Key[] keys = [seedKey];

			Record[] records = client.GetHeader(null, tokenSource.Token, keys).Result;

			Assert.AreEqual(1, records.Length);
			AssertRecordFound(seedKey, records[0]);
			Assert.IsTrue(records[0].generation > 0);
		}

		[TestMethod]
		public void AsyncBatchOperateReadWithTask()
		{
			List<BatchRead> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			List<BatchRead> result = client.Get(null, tokenSource.Token, records).Result;

			Assert.AreEqual(1, result.Count);
			Assert.AreEqual(0, result[0].resultCode);
			AssertRecordFound(seedKey, result[0].record);
			Assert.AreEqual(BinValue, result[0].record.GetString(BinName));
		}

		[TestMethod]
		public void AsyncBatchOperateListWithTask()
		{
			List<BatchRecord> records =
			[
				new BatchRead(seedKey, [BinName])
			];

			bool status = client.Operate(null, tokenSource.Token, records).Result;

			Assert.IsTrue(status);
			Assert.AreEqual(0, records[0].resultCode);
			AssertRecordFound(seedKey, records[0].record);
			Assert.AreEqual(BinValue, records[0].record.GetString(BinName));
		}

		[TestMethod]
		public void AsyncExecuteWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-task-udf");
			client.Delete(null, tokenSource.Token, key).Wait();

			object result = client.Execute(null, tokenSource.Token, key, "record_example", "writeBin",
				Value.Get(BinName), Value.Get("udf-task")).Result;

			Assert.IsNull(result);

			Record record = client.Get(null, tokenSource.Token, key, BinName).Result;
			AssertRecordFound(key, record);
			Assert.AreEqual("udf-task", record.GetString(BinName));
		}

		[TestMethod]
		public void AsyncBatchExecuteWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-task-batch-udf");
			Key[] keys = [key];
			client.Delete(new BatchPolicy(), new BatchDeletePolicy(), tokenSource.Token, keys).Wait();

			BatchResults results = client.Execute(new BatchPolicy(), new BatchUDFPolicy(), tokenSource.Token, keys,
				"record_example", "writeBin", Value.Get(BinName), Value.Get("udf-batch-task")).Result;

			Assert.IsTrue(results.status);
			Assert.AreEqual(1, results.records.Length);
			Assert.AreEqual(0, results.records[0].resultCode);

			Record record = client.Get(null, tokenSource.Token, key, BinName).Result;
			AssertRecordFound(key, record);
			Assert.AreEqual("udf-batch-task", record.GetString(BinName));
		}
	}
}
