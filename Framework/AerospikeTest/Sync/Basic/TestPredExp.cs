/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestPredExp : TestSync
	{
		private static readonly Key keyA = new Key(args.ns, args.set, "A");
		private static readonly Key keyB = new Key(args.ns, args.set, "B");

		private static readonly String binAName = "A";

		private static readonly Bin binA1 = new Bin(binAName, 1L);
		private static readonly Bin binA2 = new Bin(binAName, 2L);
		private static readonly Bin binA3 = new Bin(binAName, 3L);

		private BatchPolicy predAEq1BPolicy;
		private Policy predAEq1RPolicy;
		private WritePolicy predAEq1WPolicy;

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.Resources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestInitialize()]
		public void Initialize()
		{
			predAEq1BPolicy = new BatchPolicy();
			predAEq1RPolicy = new Policy();
			predAEq1WPolicy = new WritePolicy();

#pragma warning disable 0618
			predAEq1BPolicy.SetPredExp(
					PredExp.IntegerBin(binAName),
					PredExp.IntegerValue(1),
					PredExp.IntegerEqual());

			predAEq1RPolicy.SetPredExp(
					PredExp.IntegerBin(binAName),
					PredExp.IntegerValue(1),
					PredExp.IntegerEqual());

			predAEq1WPolicy.SetPredExp(
					PredExp.IntegerBin(binAName),
					PredExp.IntegerValue(1),
					PredExp.IntegerEqual());
#pragma warning restore 0618

			client.Delete(null, keyA);
			client.Delete(null, keyB);

			client.Put(null, keyA, binA1);
			client.Put(null, keyB, binA2);
		}

		[TestMethod]
		public void PredExpPut()
		{
			client.Put(predAEq1WPolicy, keyA, binA3);
			Record r = client.Get(null, keyA);

			AssertBinEqual(keyA, r, binA3);

			client.Put(predAEq1WPolicy, keyB, binA3);
			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA2);
		}

		[TestMethod]
		public void PredExpPutExcept()
		{
			predAEq1WPolicy.failOnFilteredOut = true;

			client.Put(predAEq1WPolicy, keyA, binA3);

			try {
				client.Put(predAEq1WPolicy, keyB, binA3);
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpGet()
		{
			Record r = client.Get(predAEq1RPolicy, keyA);

			AssertBinEqual(keyA, r, binA1);

			r = client.Get(predAEq1RPolicy, keyB);

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void PredExpGetExcept()
		{
			predAEq1RPolicy.failOnFilteredOut = true;

			client.Get(predAEq1RPolicy, keyA);

			try {
				client.Get(predAEq1RPolicy, keyB);
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpBatch()
		{
			Key[] keys = { keyA, keyB };

			Record[] records = client.Get(predAEq1BPolicy, keys);

			AssertBinEqual(keyA, records[0], binA1);
			Assert.AreEqual(null, records[1]);
		}

		[TestMethod]
		public void PredExpDelete()
		{
			client.Delete(predAEq1WPolicy, keyA);
			Record r = client.Get(null, keyA);

			Assert.AreEqual(null, r);

			client.Delete(predAEq1WPolicy, keyB);
			r = client.Get(null,  keyB);

			AssertBinEqual(keyB, r, binA2);
		}

		[TestMethod]
		public void PredExpDeleteExcept()
		{
			predAEq1WPolicy.failOnFilteredOut = true;

			client.Delete(predAEq1WPolicy, keyA);

			try {
				client.Delete(predAEq1WPolicy, keyB);
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpDurableDelete()
		{
			if (!args.enterprise)
			{
				return;
			}

			predAEq1WPolicy.durableDelete = true;

			client.Delete(predAEq1WPolicy, keyA);
			Record r = client.Get(null, keyA);

			Assert.AreEqual(null, r);

			client.Delete(predAEq1WPolicy, keyB);
			r = client.Get(null,  keyB);

			AssertBinEqual(keyB, r, binA2);
		}

		[TestMethod]
		public void PredExpDurableDeleteExcept()
		{
			if (!args.enterprise)
			{
				return;
			}

			predAEq1WPolicy.failOnFilteredOut = true;
			predAEq1WPolicy.durableDelete = true;

			client.Delete(predAEq1WPolicy, keyA);

			try {
				client.Delete(predAEq1WPolicy, keyB);
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpOperateRead()
		{
			Record r = client.Operate(predAEq1WPolicy, keyA,
					Operation.Get(binAName));

			AssertBinEqual(keyA, r, binA1);

			r = client.Operate(predAEq1WPolicy, keyB,
					Operation.Get(binAName));

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void PredExpOperateReadExcept()
		{
			predAEq1WPolicy.failOnFilteredOut = true;

			client.Operate(predAEq1WPolicy, keyA, Operation.Get(binAName));

			try {
				client.Operate(predAEq1WPolicy, keyB, Operation.Get(binAName));
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpOperateWrite()
		{
			Record r = client.Operate(predAEq1WPolicy, keyA,
					Operation.Put(binA3), Operation.Get(binAName));

			AssertBinEqual(keyA, r, binA3);

			r = client.Operate(predAEq1WPolicy, keyB,
					Operation.Put(binA3), Operation.Get(binAName));

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void PredExpOperateWriteExcept()
		{
			predAEq1WPolicy.failOnFilteredOut = true;

			client.Operate(predAEq1WPolicy, keyA,
					Operation.Put(binA3), Operation.Get(binAName));

			try {
				client.Operate(predAEq1WPolicy, keyB,
						Operation.Put(binA3), Operation.Get(binAName));
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}

		[TestMethod]
		public void PredExpUdf()
		{
			client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin",
					Value.Get(binA3.name), binA3.value);
			Record r = client.Get(null, keyA);

			AssertBinEqual(keyA, r, binA3);

			client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin",
					Value.Get(binA3.name), binA3.value);
			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA2);
		}

		[TestMethod]
		public void PredExpUdfExcept()
		{
			predAEq1WPolicy.failOnFilteredOut = true;

			client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin",
					Value.Get(binA3.name), binA3.value);

			try {
				client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin",
						Value.Get(binA3.name), binA3.value);
				Assert.Fail("Expected AerospikeException filtered out (27)");
			}
			catch (AerospikeException e) {
				Assert.AreEqual(27, e.Result);
			}
		}
	}
}
