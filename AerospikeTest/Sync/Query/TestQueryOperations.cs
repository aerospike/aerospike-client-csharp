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
using Microsoft.DiaSymReader;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryOperations : TestSync
	{
		private const string indexName = "tqoindex";
		private const string keyPrefix = "tqokey";
		private static readonly string binName1 = Suite.GetBinName("tqobin1");
		private static readonly string binName2 = Suite.GetBinName("tqobin2");
		private static readonly string binName3 = Suite.GetBinName("tqobin3");
		private static readonly string mapBin = Suite.GetBinName("tqomapbin");
		private const int size = 20;

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName1, IndexType.NUMERIC);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			for (int i = 1; i <= size; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Dictionary<Value, Value> map = new()
				{
					{ Value.Get("a"), Value.Get(i) },
					{ Value.Get("b"), Value.Get(i * 10) }
				};
				client.Put(null, key,
					new Bin(binName1, i),
					new Bin(binName2, i * 10),
					new Bin(binName3, i * 100),
					new Bin(mapBin, map));
			}
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, SuiteHelpers.set, indexName);
		}

		[TestMethod]
		public void QueryProjectMultipleBins()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);

			stmt.Operations = [
				Operation.Get(binName1),
				Operation.Get(binName2),
				MapOperation.GetByKey(mapBin, Value.Get("a"), MapReturnType.VALUE)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				Assert.IsNotNull(record.GetValue(binName1));
				Assert.IsNotNull(record.GetValue(binName2));
				Assert.IsNotNull(record.GetValue(mapBin));

				long val1 = record.GetLong(binName1);
				long val2 = record.GetLong(binName2);
				long mapVal = record.GetLong(mapBin);
				Assert.AreEqual(val1 * 10, val2);
				Assert.AreEqual(val1, mapVal);
				Assert.IsNull(record.GetValue(binName3));
				count++;
			}
			Assert.IsTrue(count >= size);
		}

		[TestMethod]
		public void QueryProjectSubsetOfBins()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			stmt.Operations = [
				Operation.Get(binName1),
				Operation.Get(binName3)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long val1 = record.GetLong(binName1);
				long val3 = record.GetLong(binName3);
				Assert.IsTrue(val1 >= begin && val1 <= end);
				Assert.AreEqual(val1 * 100, val3);
				Assert.IsNull(record.GetValue(binName2));
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryProjectBinsViaExpressionRead()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);

			Expression exp1 = Exp.Build(Exp.IntBin(binName1));
			Expression exp2 = Exp.Build(Exp.IntBin(binName2));
			Expression exp3 = Exp.Build(Exp.IntBin(binName3));

			stmt.Operations = [
				ExpOperation.Read("result1", exp1, ExpReadFlags.DEFAULT),
				ExpOperation.Read("result2", exp2, ExpReadFlags.DEFAULT),
				ExpOperation.Read("result3", exp3, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long r1 = record.GetLong("result1");
				long r2 = record.GetLong("result2");
				long r3 = record.GetLong("result3");
				Assert.AreEqual(r1 * 10, r2);
				Assert.AreEqual(r1 * 100, r3);
				count++;
			}
			Assert.IsTrue(count >= size);
		}

		[TestMethod]
		public void QueryProjectBinsViaExpressionReadWithFilter()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression exp1 = Exp.Build(Exp.IntBin(binName1));
			Expression exp2 = Exp.Build(Exp.IntBin(binName2));
			Expression exp3 = Exp.Build(Exp.IntBin(binName3));

			stmt.Operations = [
				ExpOperation.Read("result1", exp1, ExpReadFlags.DEFAULT),
				ExpOperation.Read("result2", exp2, ExpReadFlags.DEFAULT),
				ExpOperation.Read("result3", exp3, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long r1 = record.GetLong("result1");
				long r2 = record.GetLong("result2");
				long r3 = record.GetLong("result3");
				Assert.IsTrue(r1 >= begin && r1 <= end);
				Assert.AreEqual(r1 * 10, r2);
				Assert.AreEqual(r1 * 100, r3);
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryProjectMixedGetAndExpressionRead()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression computedExp = Exp.Build(
				Exp.Add(Exp.IntBin(binName1), Exp.IntBin(binName2))
			);

			stmt.Operations = [
				Operation.Get(binName1),
				ExpOperation.Read("sum", computedExp, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long val1 = record.GetLong(binName1);
				long sum = record.GetLong("sum");
				Assert.IsTrue(val1 >= begin && val1 <= end);
				Assert.AreEqual(val1 + val1 * 10, sum);
				Assert.IsNull(record.GetValue(binName2));
				Assert.IsNull(record.GetValue(binName3));
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryWithExpReadOperation()
		{
			int begin = 1;
			int end = 10;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression exp = Exp.Build(
				Exp.Mul(Exp.IntBin(binName1), Exp.Val(100))
			);

			stmt.Operations = [
				Operation.Get(binName1),
				ExpOperation.Read("computed", exp, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long computed = record.GetLong("computed");
				long original = record.GetLong(binName1);
				Assert.AreEqual(original * 100, computed);
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryWithMultipleExpReadOperations()
		{
			int begin = 5;
			int end = 15;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression sumExp = Exp.Build(
				Exp.Add(Exp.IntBin(binName1), Exp.IntBin(binName2))
			);
			Expression diffExp = Exp.Build(
				Exp.Sub(Exp.IntBin(binName2), Exp.IntBin(binName1))
			);

			stmt.Operations = [
				Operation.Get(binName1),
				Operation.Get(binName2),
				ExpOperation.Read("sum", sumExp, ExpReadFlags.DEFAULT),
				ExpOperation.Read("diff", diffExp, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long val1 = record.GetLong(binName1);
				long val2 = record.GetLong(binName2);
				long sum = record.GetLong("sum");
				long diff = record.GetLong("diff");
				Assert.AreEqual(val1 + val2, sum);
				Assert.AreEqual(val2 - val1, diff);
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryWithExpReadAndFilterExp()
		{
			int begin = 1;
			int end = 20;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression computedExp = Exp.Build(
				Exp.Mul(Exp.IntBin(binName1), Exp.Val(2))
			);

			stmt.Operations = [
				Operation.Get(binName1),
				ExpOperation.Read("doubled", computedExp, ExpReadFlags.DEFAULT)
			];

			QueryPolicy policy = new()
			{
				filterExp = Exp.Build(
					Exp.LT(Exp.IntBin(binName1), Exp.Val(6))
				)
			};

			using RecordSet rs = client.Query(policy, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long doubled = record.GetLong("doubled");
				long original = record.GetLong(binName1);
				Assert.AreEqual(original * 2, doubled);
				Assert.IsTrue(original < 6);
				count++;
			}
			Assert.AreEqual(5, count);
		}

		[TestMethod]
		public void QueryWithGetOperation()
		{
			int begin = 1;
			int end = 5;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			stmt.Operations = [Operation.Get(binName1)];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				long val1 = record.GetLong(binName1);
				Assert.IsTrue(val1 >= begin && val1 <= end);
				Assert.IsNull(record.GetValue(binName2));
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void QueryRejectsWriteOperation()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			Bin bin = new("foo", "bar");
			stmt.Operations = [Operation.Put(bin)];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for write operation in foreground query");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "read-only");
			}
		}

		[TestMethod]
		public void QueryRejectsExpWriteOperation()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			Expression exp = Exp.Build(Exp.Val("bar"));
			stmt.Operations = [ExpOperation.Write("foo", exp, ExpWriteFlags.DEFAULT)];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for ExpWrite in foreground query");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "read-only");
			}
		}

		[TestMethod]
		public void QueryRejectsMixedReadWriteOperations()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			Expression readExp = Exp.Build(Exp.IntBin(binName1));
			Expression writeExp = Exp.Build(Exp.Val("updated"));

			stmt.Operations = [
				ExpOperation.Read("computed", readExp, ExpReadFlags.DEFAULT),
				ExpOperation.Write("foo", writeExp, ExpWriteFlags.DEFAULT)
			];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for mixed ops in foreground query");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "read-only");
			}
		}

		[TestMethod]
		public void ExecuteRejectsReadOnlyOperations()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			Expression exp = Exp.Build(Exp.IntBin(binName1));

			try
			{
				client.Execute(null, stmt, ExpOperation.Read("computed", exp, ExpReadFlags.DEFAULT));
				Assert.Fail("Expected AerospikeException for read-only ops in background execute");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "write");
			}
		}

		[TestMethod]
		public void ExecuteWithWriteOperationSucceeds()
		{
			int begin = 1;
			int end = 3;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression exp = Exp.Build(Exp.Val("executed"));
			ExecuteTask task = client.Execute(null, stmt,
				ExpOperation.Write("marker", exp, ExpWriteFlags.DEFAULT)
			);
			task.Wait(3000, 3000);

			for (int i = begin; i <= end; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, keyPrefix + i);
				Record record = client.Get(null, key);
				Assert.IsNotNull(record);
				Assert.AreEqual("executed", record.GetString("marker"));
			}
		}

		[TestMethod]
		public void ExecuteRejectsMixedReadWriteOperations()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			Expression readExp = Exp.Build(Exp.IntBin(binName1));
			Expression writeExp = Exp.Build(Exp.Val("mixed"));

			try
			{
				client.Execute(null, stmt,
					ExpOperation.Read("computed", readExp, ExpReadFlags.DEFAULT),
					ExpOperation.Write("tag", writeExp, ExpWriteFlags.DEFAULT)
				);
				Assert.Fail("Expected AerospikeException for mixed read/write ops in background execute");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "write-only");
			}
		}

		[TestMethod]
		public void QueryWithExpReadNoFilter()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);

			Expression exp = Exp.Build(
				Exp.Add(Exp.IntBin(binName1), Exp.Val(1000))
			);

			stmt.Operations = [ExpOperation.Read("offset", exp, ExpReadFlags.DEFAULT)];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				object offsetVal = record.GetValue("offset");
				Assert.IsNotNull(offsetVal);
				count++;
			}
			Assert.IsTrue(count >= size);
		}

		[TestMethod]
		public void QueryWithExpReadConditional()
		{
			int begin = 1;
			int end = 20;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression exp = Exp.Build(
				Exp.Cond(
					Exp.GT(Exp.IntBin(binName1), Exp.Val(10)), Exp.Val("high"),
					Exp.Val("low")
				)
			);

			stmt.Operations = [
				Operation.Get(binName1),
				ExpOperation.Read("category", exp, ExpReadFlags.DEFAULT)
			];

			using RecordSet rs = client.Query(null, stmt);

			int highCount = 0;
			int lowCount = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				string category = record.GetString("category");
				long val = record.GetLong(binName1);
				Assert.IsNotNull(category);

				if (val > 10)
				{
					Assert.AreEqual("high", category);
					highCount++;
				}
				else
				{
					Assert.AreEqual("low", category);
					lowCount++;
				}
			}
			Assert.AreEqual(10, highCount);
			Assert.AreEqual(10, lowCount);
		}

		[TestMethod]
		public void QueryRejectsTouchOperation()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			stmt.Operations = [Operation.Touch()];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for Touch in foreground query");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "read-only");
			}
		}

		[TestMethod]
		public void QueryRejectsDeleteOperation()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			stmt.Operations = [Operation.Delete()];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException for Delete in foreground query");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "read-only");
			}
		}

		[TestMethod]
		public void QueryWithExpReadEvalNoFail()
		{
			int begin = 1;
			int end = 5;

			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, begin, end));

			Expression exp = Exp.Build(Exp.IntBin("nonexistent"));
			stmt.Operations = [
				Operation.Get(binName1),
				ExpOperation.Read("result", exp, ExpReadFlags.EVAL_NO_FAIL)
			];

			using RecordSet rs = client.Query(null, stmt);

			int count = 0;

			while (rs.Next())
			{
				Record record = rs.Record;
				Assert.IsNotNull(record.GetValue(binName1));
				count++;
			}
			Assert.AreEqual(end - begin + 1, count);
		}

		[TestMethod]
		public void ExecuteRejectsGetOperation()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));

			try
			{
				client.Execute(null, stmt, Operation.Get(binName1));
				Assert.Fail("Expected AerospikeException for read-only Get in background execute");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "write");
			}
		}

		[TestMethod]
		public void QueryRejectsBinNamesWithOperations()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));
			stmt.SetBinNames(binName1, binName2);
			stmt.Operations = [Operation.Get(binName1)];

			try
			{
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
				}
				Assert.Fail("Expected AerospikeException when both binNames and operations are set");
			}
			catch (AerospikeException ae)
			{
				AerospikeException inner = ae.InnerException as AerospikeException;
				Test.AssertParameterError(inner, "bin names");
			}
		}

		[TestMethod]
		public void ExecuteRejectsBinNamesWithOperations()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(SuiteHelpers.set);
			stmt.SetFilter(Filter.Range(binName1, 1, 5));
			stmt.SetBinNames(binName1, binName2);

			Expression writeExp = Exp.Build(Exp.Val("tagged"));

			try
			{
				client.Execute(null, stmt,
					ExpOperation.Write("tag", writeExp, ExpWriteFlags.DEFAULT));
				Assert.Fail("Expected AerospikeException when both binNames and operations are set");
			}
			catch (AerospikeException ae)
			{
				Test.AssertParameterError(ae, "bin names");
			}
		}
	}
}
