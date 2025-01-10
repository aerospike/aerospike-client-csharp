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
using System;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestExpOperation : TestSync
	{
		readonly string binA = "A";
		readonly string binB = "B";
		readonly string binC = "C";
		readonly string binD = "D";
		readonly string binH = "H";
		readonly string expVar = "EV";

		readonly Key keyA = new(SuiteHelpers.ns, SuiteHelpers.set, "A");
		readonly Key keyB = new(SuiteHelpers.ns, SuiteHelpers.set, "B"u8.ToArray());

		[TestInitialize()]
		public void SetUp()
		{
			client.Delete(null, keyA);
			client.Delete(null, keyB);

			client.Put(null, keyA, new Bin(binA, 1), new Bin(binD, 2));
			client.Put(null, keyB, new Bin(binB, 2), new Bin(binD, 2));
		}

		[TestMethod]
		public void ExpReadEvalError()
		{
			Expression exp = Exp.Build(Exp.Add(Exp.IntBin(binA), Exp.Val(4)));

			Record record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));
			AssertRecordFound(keyA, record);

			Test.TestException(() => 
			{
				client.Operate(null, keyB, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));
			}, ResultCode.OP_NOT_APPLICABLE);

			record = client.Operate(null, keyB, ExpOperation.Read(expVar, exp, ExpReadFlags.EVAL_NO_FAIL));
			AssertRecordFound(keyB, record);
		}

		[TestMethod]
		public void ExpReadOnWriteEvalError()
		{
			Expression wexp = Exp.Build(Exp.IntBin(binD));
			Expression rexp = Exp.Build(Exp.IntBin(binA));

			Record record = client.Operate(null, keyA, 
				ExpOperation.Write(binD, wexp, ExpWriteFlags.DEFAULT), 
				ExpOperation.Read(expVar, rexp, ExpReadFlags.DEFAULT)
				);
			AssertRecordFound(keyA, record);

			Test.TestException(() =>
			{
				client.Operate(null, keyB, 
					ExpOperation.Write(binD, wexp, ExpWriteFlags.DEFAULT),
					ExpOperation.Read(expVar, rexp, ExpReadFlags.DEFAULT));
			}, ResultCode.OP_NOT_APPLICABLE);

			record = client.Operate(null, keyB,
				ExpOperation.Read(expVar, rexp, ExpReadFlags.EVAL_NO_FAIL));
			AssertRecordFound(keyB, record);
		}

		[TestMethod]
		public void ExpWriteEvalError()
		{
			Expression wexp = Exp.Build(Exp.Add(Exp.IntBin(binA), Exp.Val(4)));
			Expression rexp = Exp.Build(Exp.IntBin(binC));

			Record record = client.Operate(null, keyA, 
				ExpOperation.Write(binC, wexp, ExpWriteFlags.DEFAULT),
				ExpOperation.Read(expVar, rexp, ExpReadFlags.DEFAULT)
				);
			AssertRecordFound(keyA, record);

			Test.TestException(() => {
				client.Operate(null, keyB,
					ExpOperation.Write(binC, wexp, ExpWriteFlags.DEFAULT),
					ExpOperation.Read(expVar, rexp, ExpReadFlags.DEFAULT));
			}, ResultCode.OP_NOT_APPLICABLE);

			record = client.Operate(null, keyB,
				ExpOperation.Write(binC, wexp, ExpWriteFlags.EVAL_NO_FAIL),
				ExpOperation.Read(expVar, rexp, ExpReadFlags.EVAL_NO_FAIL));
			AssertRecordFound(keyB, record);
		}

		[TestMethod]
		public void ExpWritePolicyError()
		{
			Expression wexp = Exp.Build(Exp.Add(Exp.IntBin(binA), Exp.Val(4)));

			Test.TestException(() => {
				client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.UPDATE_ONLY));
			}, ResultCode.BIN_NOT_FOUND);

			Record record = client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.UPDATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL));
			AssertRecordFound(keyA, record);

			record = client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.CREATE_ONLY));
			AssertRecordFound(keyA, record);

			Test.TestException(() => {
				client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.CREATE_ONLY));
			}, ResultCode.BIN_EXISTS_ERROR);

			record = client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL));
			AssertRecordFound(keyA, record);

			Expression dexp = Exp.Build(Exp.Nil());

			Test.TestException(() => {
				client.Operate(null, keyA, ExpOperation.Write(binC, dexp, ExpWriteFlags.DEFAULT));
			}, ResultCode.OP_NOT_APPLICABLE);

			record = client.Operate(null, keyA, ExpOperation.Write(binC, dexp, ExpWriteFlags.POLICY_NO_FAIL));
			AssertRecordFound(keyA, record);

			record = client.Operate(null, keyA, ExpOperation.Write(binC, dexp, ExpWriteFlags.ALLOW_DELETE));
			AssertRecordFound(keyA, record);

			record = client.Operate(null, keyA, ExpOperation.Write(binC, wexp, ExpWriteFlags.CREATE_ONLY));
			AssertRecordFound(keyA, record);
		}

		[TestMethod]
		public void ExpReturnsUnknown()
		{
			Expression exp = Exp.Build(
				Exp.Cond(
					Exp.EQ(Exp.IntBin(binC), Exp.Val(5)), Exp.Unknown(),
					Exp.BinExists(binA), Exp.Val(5), Exp.Unknown()));

			Test.TestException(() => {
				client.Operate(null, keyA,
					ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
					Operation.Get(binC));
			}, ResultCode.OP_NOT_APPLICABLE);

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.EVAL_NO_FAIL),
				Operation.Get(binC));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			object val = results[0];
			Assert.IsNull(val);
			val = results[1];
			Assert.IsNull(val);
		}

		[TestMethod]
		public void ExpReturnsNil()
		{
			Expression exp = Exp.Build(Exp.Nil());

			Record record = client.Operate(null, keyA,
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT),
				Operation.Get(binC));

			AssertRecordFound(keyA, record);

			object val = record.GetValue(expVar);
			Assert.IsNull(val);
		}

		[TestMethod]
		public void ExpReturnsInt()
		{
			Expression exp = Exp.Build(Exp.Add(Exp.IntBin(binA), Exp.Val(4)));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC), 
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			long val = (long)results[1];
			Assert.AreEqual(5, val);

			val = record.GetLong(expVar);
			Assert.AreEqual(5, val);

			record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			val = record.GetLong(expVar);
			Assert.AreEqual(5, val);
		}

		[TestMethod]
		public void ExpReturnsFloat()
		{
			Expression exp = Exp.Build(Exp.Add(Exp.ToFloat(Exp.IntBin(binA)), Exp.Val(4.0)));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			double val = (double)results[1];
			double delta = 0.000001;
			Assert.AreEqual(5.0, val, delta);

			val = record.GetDouble(expVar);
			Assert.AreEqual(5.0, val, delta);

			record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			val = record.GetDouble(expVar);
			Assert.AreEqual(5.0, val, delta);
		}

		[TestMethod]
		public void ExpReturnsString()
		{
			string str = "aaa";
			Expression exp = Exp.Build(Exp.Val(str));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			string val = (string)results[1];
			Assert.AreEqual(str, val);

			val = record.GetString(expVar);
			Assert.AreEqual(str, val);

			record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			val = record.GetString(expVar);
			Assert.AreEqual(str, val);
		}

		[TestMethod]
		public void ExpReturnsBlob()
		{
			byte[] bytes = [0x78, 0x78, 0x78];
			Expression exp = Exp.Build(Exp.Val(bytes));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			byte[] val = (byte[])results[1];
			Assert.IsTrue(Util.ByteArrayEquals(bytes, val));

			val = (byte[])record.GetValue(expVar);
			Assert.IsTrue(Util.ByteArrayEquals(bytes, val));

			record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			val = (byte[])record.GetValue(expVar);
			Assert.IsTrue(Util.ByteArrayEquals(bytes, val));
		}

		[TestMethod]
		public void ExpReturnsBoolean()
		{
			Expression exp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binC);
			bool val = (bool)results[1];
			Assert.IsTrue(val);

			val = record.GetBool(expVar);
			Assert.IsTrue(val);
		}

		[TestMethod]
		public void ExpReturnsHLL()
		{
			Expression exp = Exp.Build(HLLExp.Init(HLLPolicy.Default, Exp.Val(4), Exp.Nil()));

			Record record = client.Operate(null, keyA,
				HLLOperation.Init(HLLPolicy.Default, binH, 4),
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binH),
				Operation.Get(binC),
				ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			IList results = record.GetList(binH);
			Value.HLLValue valH = (Value.HLLValue)results[1];

			results = record.GetList(binC);
			Value.HLLValue valC = (Value.HLLValue)results[1];

			Value.HLLValue valExp = record.GetHLLValue(expVar);

			Assert.IsTrue(Util.ByteArrayEquals((byte[])valH.Object, (byte[])valC.Object));
			Assert.IsTrue(Util.ByteArrayEquals((byte[])valH.Object, (byte[])valExp.Object));

			record = client.Operate(null, keyA, ExpOperation.Read(expVar, exp, ExpReadFlags.DEFAULT));

			valExp = record.GetHLLValue(expVar);
			Assert.IsTrue(Util.ByteArrayEquals((byte[])valH.Object, (byte[])valExp.Object));
		}

		[TestMethod]
		public void ExpMerge()
		{
			Expression e = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(0)));
			Expression eand = Exp.Build(Exp.And(Exp.Expr(e), Exp.EQ(Exp.IntBin(binD), Exp.Val(2))));
			Expression eor = Exp.Build(Exp.Or(Exp.Expr(e), Exp.EQ(Exp.IntBin(binD), Exp.Val(2))));

			Record record = client.Operate(null, keyA,
				ExpOperation.Read("res1", eand, ExpReadFlags.DEFAULT),
				ExpOperation.Read("res2", eor, ExpReadFlags.DEFAULT));

			AssertRecordFound(keyA, record);

			bool res1 = record.GetBool("res1");
			Assert.IsFalse(res1);

			bool res2 = record.GetBool("res2");
			Assert.IsTrue(res2);
		}

		[TestMethod]
		public void ExpRowBatchRead()
		{
			List<BatchRecord> records = [];
			Expression expr = Exp.Build(Exp.EQ(Exp.IntBin("count"), Exp.Val(0)));

			for (int i = 0; i < 1; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, i);
				client.Put(null, key, new Bin("count", i));

				BatchWritePolicy bwp = new()
				{
					filterExp = expr
				};
				records.Add(
					new BatchWrite(bwp, key, [
				Operation.Put(new Bin("age", 10)),
				Operation.Get("age"),
					])
				);
			}

			BatchPolicy bp = new()
			{
				failOnFilteredOut = true
			};
			bool isSuccess = client.Operate(bp, records);
			Assert.IsTrue(isSuccess);
		}
	}
}
