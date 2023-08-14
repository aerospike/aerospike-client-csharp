/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestFilterExp : TestSync
	{
		string binA = "A";
		string binB = "B";
		string binC = "C";
		string binD = "D";
		string binE = "E";

		Key keyA = new Key(args.ns, args.set, "A");
		Key keyB = new Key(args.ns, args.set, new byte[] {(byte)'B'});
		Key keyC = new Key(args.ns, args.set, "C");

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestInitialize()]
		public void Initialize()
		{
			client.Delete(null, keyA);
			client.Delete(null, keyB);
			client.Delete(null, keyC);

			client.Put(null, keyA, new Bin(binA, 1), new Bin(binB, 1.1), new Bin(binC, "abcde"), new Bin(binD, 1), new Bin(binE, -1));
			client.Put(null, keyB, new Bin(binA, 2), new Bin(binB, 2.2), new Bin(binC, "abcdeabcde"), new Bin(binD, 1), new Bin(binE, -2));
			client.Put(null, keyC, new Bin(binA, 0), new Bin(binB, -1), new Bin(binC, 1));
		}

		[TestMethod]
		public void FilterExpPut()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Bin bin = new Bin(binA, 3);

			client.Put(policy, keyA, bin);
			Record r = client.Get(null, keyA);

			AssertBinEqual(keyA, r, binA, 3);

			client.Put(policy, keyB, bin);
			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpPutExcept()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			Bin bin = new Bin(binA, 3);

			client.Put(policy, keyA, bin);

			Test.TestException(() =>
			{
				client.Put(policy, keyB, bin);
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpGet()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Record r = client.Get(policy, keyA);

			AssertBinEqual(keyA, r, binA, 1);

			r = client.Get(policy, keyB);

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void FilterExpGetExcept()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			client.Get(policy, keyA);

			Test.TestException(() =>
			{
				client.Get(policy, keyB);
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpBatch()
		{
			BatchPolicy policy = new BatchPolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Key[] keys = new Key[] { keyA, keyB };

			Record[] records = client.Get(policy, keys);

			AssertBinEqual(keyA, records[0], binA, 1);
			Assert.AreEqual(null, records[1]);
		}

		[TestMethod]
		public void FilterExpDelete()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			client.Delete(policy, keyA);
			Record r = client.Get(null, keyA);

			Assert.AreEqual(null, r);

			client.Delete(policy, keyB);
			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpDeleteExcept()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			client.Delete(policy, keyA);

			Test.TestException(() =>
			{
				client.Delete(policy, keyB);
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpDurableDelete()
		{
			if (!args.enterprise)
			{
				return;
			}

			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.durableDelete = true;

			client.Delete(policy, keyA);
			Record r = client.Get(null, keyA);

			Assert.AreEqual(null, r);

			client.Delete(policy, keyB);
			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpDurableDeleteExcept()
		{
			if (!args.enterprise)
			{
				return;
			}

			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;
			policy.durableDelete = true;

			client.Delete(policy, keyA);

			Test.TestException(() =>
			{
				client.Delete(policy, keyB);
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpOperateRead()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Record r = client.Operate(policy, keyA, Operation.Get(binA));

			AssertBinEqual(keyA, r, binA, 1);

			r = client.Operate(policy, keyB, Operation.Get(binA));

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void FilterExpOperateReadExcept()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			client.Operate(policy, keyA, Operation.Get(binA));

			Test.TestException(() =>
			{
				client.Operate(policy, keyB, Operation.Get(binA));
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpOperateWrite()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			Bin bin = new Bin(binA, 3);

			Record r = client.Operate(policy, keyA, Operation.Put(bin), Operation.Get(binA));

			AssertBinEqual(keyA, r, binA, 3);

			r = client.Operate(policy, keyB, Operation.Put(bin), Operation.Get(binA));

			Assert.AreEqual(null, r);
		}

		[TestMethod]
		public void FilterExpOperateWriteExcept()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			Bin bin = new Bin(binA, 3);

			client.Operate(policy, keyA, Operation.Put(bin), Operation.Get(binA));

			Test.TestException(() =>
			{
				client.Operate(policy, keyB, Operation.Put(bin), Operation.Get(binA));
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpUdf()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			client.Execute(policy, keyA, "record_example", "writeBin", Value.Get(binA), Value.Get(3));

			Record r = client.Get(null, keyA);

			AssertBinEqual(keyA, r, binA, 3);

			client.Execute(policy, keyB, "record_example", "writeBin", Value.Get(binA), Value.Get(3));

			r = client.Get(null, keyB);

			AssertBinEqual(keyB, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpUdfExcept()
		{
			WritePolicy policy = new WritePolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));
			policy.failOnFilteredOut = true;

			client.Execute(policy, keyA, "record_example", "writeBin", Value.Get(binA), Value.Get(3));

			Test.TestException(() =>
			{
				client.Execute(policy, keyB, "record_example", "writeBin", Value.Get(binA), Value.Get(3));
			}, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void FilterExpFilterExclusive()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.Exclusive(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)), Exp.EQ(Exp.IntBin(binD), Exp.Val(1))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterAddInt()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Add(Exp.IntBin(binA), Exp.IntBin(binD), Exp.Val(1)),
					Exp.Val(4)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterAddFloat()
		{
			string name = "val";

			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Let(
					Exp.Def(name, Exp.Add(Exp.FloatBin(binB), Exp.Val(1.1))), 
					Exp.And(
						Exp.GE(Exp.Var(name), Exp.Val(3.2999)), 
						Exp.LE(Exp.Var(name), Exp.Val(3.3001)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterSub()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Sub(Exp.Val(1), Exp.IntBin(binA), Exp.IntBin(binD)),
					Exp.Val(-2)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterMul()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Mul(Exp.Val(2), Exp.IntBin(binA), Exp.IntBin(binD)),
					Exp.Val(4)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterDiv()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Div(Exp.Val(8), Exp.IntBin(binA), Exp.IntBin(binD)),
					Exp.Val(4)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterPow()
		{
			string name = "x";

			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Let(
					Exp.Def(name, Exp.Pow(Exp.FloatBin(binB), Exp.Val(2.0))),
					Exp.And(
						Exp.GE(Exp.Var(name), Exp.Val(4.8399)),
						Exp.LE(Exp.Var(name), Exp.Val(4.8401)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterLog()
		{
			string name = "x";

			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Let(
					Exp.Def(name, Exp.Log(Exp.FloatBin(binB), Exp.Val(2.0))),
					Exp.And(
						Exp.GE(Exp.Var(name), Exp.Val(1.1374)),
						Exp.LE(Exp.Var(name), Exp.Val(1.1376)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterMod()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Mod(Exp.IntBin(binA), Exp.Val(2)),
					Exp.Val(0)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterAbs()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.Abs(Exp.IntBin(binE)), Exp.Val(2)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterFloor()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.Floor(Exp.FloatBin(binB)), Exp.Val(2.0)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterCeil()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.Ceil(Exp.FloatBin(binB)), Exp.Val(3.0)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterToInt()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.ToInt(Exp.FloatBin(binB)), Exp.Val(2)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterToFloat()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.ToFloat(Exp.IntBin(binA)), Exp.Val(2.0)));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyA, r, binA, 2);
		}

		[TestMethod]
		public void FilterExpFilterIntAnd()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.And(
						Exp.EQ(Exp.IntAnd(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(0)),
						Exp.EQ(Exp.IntAnd(Exp.IntBin(binA), Exp.Val(0xFFFF)), Exp.Val(1)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.EQ(Exp.IntAnd(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(0)),
					Exp.EQ(Exp.IntAnd(Exp.IntBin(binA), Exp.Val(0xFFFF)), Exp.Val(1))));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterIntOr()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.And(
						Exp.EQ(Exp.IntOr(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(1)),
						Exp.EQ(Exp.IntOr(Exp.IntBin(binA), Exp.Val(0xFF)), Exp.Val(0xFF)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.EQ(Exp.IntOr(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(1)),
					Exp.EQ(Exp.IntOr(Exp.IntBin(binA), Exp.Val(0xFF)), Exp.Val(0xFF))));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterIntXor()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.And(
						Exp.EQ(Exp.IntXor(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(1)),
						Exp.EQ(Exp.IntXor(Exp.IntBin(binA), Exp.Val(0xFF)), Exp.Val(0xFE)))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.EQ(Exp.IntXor(Exp.IntBin(binA), Exp.Val(0)), Exp.Val(1)),
					Exp.EQ(Exp.IntXor(Exp.IntBin(binA), Exp.Val(0xFF)), Exp.Val(0xFE))));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterIntNot()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.IntNot(Exp.IntBin(binA)), Exp.Val(-2))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.IntNot(Exp.IntBin(binA)), Exp.Val(-2)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterLshift()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.Lshift(Exp.IntBin(binA), Exp.Val(2)), Exp.Val(4))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.Lshift(Exp.IntBin(binA), Exp.Val(2)), Exp.Val(4)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterRshift()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.Rshift(Exp.IntBin(binE), Exp.Val(62)), Exp.Val(3))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyB);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.Rshift(Exp.IntBin(binE), Exp.Val(62)), Exp.Val(3)));

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyB, r, binE, -2);
		}

		[TestMethod]
		public void FilterExpFilterARshift()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.ARshift(Exp.IntBin(binE), Exp.Val(62)), Exp.Val(-1))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyB);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.ARshift(Exp.IntBin(binE), Exp.Val(62)), Exp.Val(-1)));

			Record r = client.Get(policy, keyB);
			AssertBinEqual(keyB, r, binE, -2);
		}

		[TestMethod]
		public void FilterExpFilterBitCount()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(Exp.EQ(Exp.Count(Exp.IntBin(binA)), Exp.Val(1))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.Count(Exp.IntBin(binA)), Exp.Val(1)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterLscan()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.Lscan(Exp.IntBin(binA), Exp.Val(true)), Exp.Val(63))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.Lscan(Exp.IntBin(binA), Exp.Val(true)), Exp.Val(63)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterRscan()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(Exp.Rscan(Exp.IntBin(binA), Exp.Val(true)), Exp.Val(63))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(Exp.Rscan(Exp.IntBin(binA), Exp.Val(true)), Exp.Val(63)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterMin()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(
						Exp.Min(Exp.IntBin(binA), Exp.IntBin(binD), Exp.IntBin(binE)),
						Exp.Val(-1))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Min(Exp.IntBin(binA), Exp.IntBin(binD), Exp.IntBin(binE)),
					Exp.Val(-1)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterMax()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(
						Exp.Max(Exp.IntBin(binA), Exp.IntBin(binD), Exp.IntBin(binE)),
						Exp.Val(1))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Max(Exp.IntBin(binA), Exp.IntBin(binD), Exp.IntBin(binE)),
					Exp.Val(1)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void FilterExpFilterCond()
		{
			Policy policy = new Policy();
			policy.filterExp = Exp.Build(
				Exp.Not(
					Exp.EQ(
						Exp.Cond(
							Exp.EQ(Exp.IntBin(binA), Exp.Val(0)), Exp.Add(Exp.IntBin(binD), Exp.IntBin(binE)),
							Exp.EQ(Exp.IntBin(binA), Exp.Val(1)), Exp.Sub(Exp.IntBin(binD), Exp.IntBin(binE)),
							Exp.EQ(Exp.IntBin(binA), Exp.Val(2)), Exp.Mul(Exp.IntBin(binD), Exp.IntBin(binE)),
							Exp.Val(-1)),
						Exp.Val(2))));
			policy.failOnFilteredOut = true;

			Test.TestException(() =>
			{
				client.Get(policy, keyA);
			}, ResultCode.FILTERED_OUT);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					Exp.Cond(
						Exp.EQ(Exp.IntBin(binA), Exp.Val(0)), Exp.Add(Exp.IntBin(binD), Exp.IntBin(binE)),
						Exp.EQ(Exp.IntBin(binA), Exp.Val(1)), Exp.Sub(Exp.IntBin(binD), Exp.IntBin(binE)),
						Exp.EQ(Exp.IntBin(binA), Exp.Val(2)), Exp.Mul(Exp.IntBin(binD), Exp.IntBin(binE)),
						Exp.Val(-1)),
					Exp.Val(2)));

			Record r = client.Get(policy, keyA);
			AssertBinEqual(keyA, r, binA, 1);
		}

		[TestMethod]
		public void BatchKeyFilter()
		{
			// Write/Delete records with filter.
			BatchWritePolicy wp = new BatchWritePolicy();
			wp.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(1)));

			BatchDeletePolicy dp = new BatchDeletePolicy();
			dp.filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binA), Exp.Val(0)));

			Operation[] put = Operation.Array(Operation.Put(new Bin(binA, 3)));

			List<BatchRecord> brecs = new List<BatchRecord>();
			brecs.Add(new BatchWrite(wp, keyA, put));
			brecs.Add(new BatchWrite(wp, keyB, put));
			brecs.Add(new BatchDelete(dp, keyC));

			bool status = client.Operate(null, brecs);
			Assert.IsFalse(status); // Filtered out result code causes status to be false.

			BatchRecord br = brecs[0];
			Assert.AreEqual(ResultCode.OK, br.resultCode);

			br = brecs[1];
			Assert.AreEqual(ResultCode.FILTERED_OUT, br.resultCode);

			br = brecs[2];
			Assert.AreEqual(ResultCode.OK, br.resultCode);

			// Read records
			Key[] keys = new Key[] { keyA, keyB, keyC };
			Record[] recs = client.Get(null, keys, binA);

			Record r = recs[0];
			AssertBinEqual(keyA, r, binA, 3);

			r = recs[1];
			AssertBinEqual(keyB, r, binA, 2);

			r = recs[2];
			Assert.IsNull(r);
		}
	}
}
