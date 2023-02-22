﻿/* 
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
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestHLLExp : TestSync
	{
		private string bin1 = "hllbin_1";
		private string bin2 = "hllbin_2";
		private string bin3 = "hllbin_3";
		private Policy policy = new Policy();
		private Value.HLLValue hll1;
		private Value.HLLValue hll2;
		private Value.HLLValue hll3;

		[TestMethod]
		public void HllExp()
		{
			Key key = new Key(args.ns, args.set, 5200);
			client.Delete(null, key);

			List<Value> list1 = new List<Value>();
			list1.Add(Value.Get("Akey1"));
			list1.Add(Value.Get("Akey2"));
			list1.Add(Value.Get("Akey3"));

			List<Value> list2 = new List<Value>();
			list2.Add(Value.Get("Bkey1"));
			list2.Add(Value.Get("Bkey2"));
			list2.Add(Value.Get("Bkey3"));

			List<Value> list3 = new List<Value>();
			list3.Add(Value.Get("Akey1"));
			list3.Add(Value.Get("Akey2"));
			list3.Add(Value.Get("Bkey1"));
			list3.Add(Value.Get("Bkey2"));
			list3.Add(Value.Get("Ckey1"));
			list3.Add(Value.Get("Ckey2"));

			Record rec = client.Operate(null, key,
				HLLOperation.Add(HLLPolicy.Default, bin1, list1, 8),
				HLLOperation.Add(HLLPolicy.Default, bin2, list2, 8),
				HLLOperation.Add(HLLPolicy.Default, bin3, list3, 8),
				Operation.Get(bin1),
				Operation.Get(bin2),
				Operation.Get(bin3)
				);

			IList results = rec.GetList(bin1);
			hll1 = (Value.HLLValue)results[1];
			Assert.AreNotEqual(null, hll1);

			results = rec.GetList(bin2);
			hll2 = (Value.HLLValue)results[1];
			Assert.AreNotEqual(null, hll2);

			results = rec.GetList(bin3);
			hll3 = (Value.HLLValue)results[1];
			Assert.AreNotEqual(null, hll3);

			Count(key);
			Union(key);
			Intersect(key);
			Similarity(key);
			Describe(key);
			MayContain(key);
			Add(key);
		}

		private void Count(Key key)
		{
			policy.filterExp = Exp.Build(Exp.EQ(HLLExp.GetCount(Exp.HLLBin(bin1)), Exp.Val(0)));
			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(Exp.GT(HLLExp.GetCount(Exp.HLLBin(bin1)), Exp.Val(0)));
			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Union(Key key)
		{
			List<Value.HLLValue> hlls = new List<Value.HLLValue>();
			hlls.Add(hll1);
			hlls.Add(hll2);
			hlls.Add(hll3);

			policy.filterExp = Exp.Build(
				Exp.NE(
					HLLExp.GetCount(HLLExp.GetUnion(Exp.Val(hlls), Exp.HLLBin(bin1))),
					HLLExp.GetUnionCount(Exp.Val(hlls), Exp.HLLBin(bin1))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					HLLExp.GetCount(HLLExp.GetUnion(Exp.Val(hlls), Exp.HLLBin(bin1))),
					HLLExp.GetUnionCount(Exp.Val(hlls), Exp.HLLBin(bin1))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Intersect(Key key)
		{
			List<Value.HLLValue> hlls2 = new List<Value.HLLValue>();
			hlls2.Add(hll2);

			List<Value.HLLValue> hlls3 = new List<Value.HLLValue>();
			hlls3.Add(hll3);

			policy.filterExp = Exp.Build(
				Exp.GE(
					HLLExp.GetIntersectCount(Exp.Val(hlls2), Exp.HLLBin(bin1)),
					HLLExp.GetIntersectCount(Exp.Val(hlls3), Exp.HLLBin(bin1))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.LE(
					HLLExp.GetIntersectCount(Exp.Val(hlls2), Exp.HLLBin(bin1)),
					HLLExp.GetIntersectCount(Exp.Val(hlls3), Exp.HLLBin(bin1))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Similarity(Key key)
		{
			List<Value.HLLValue> hlls2 = new List<Value.HLLValue>();
			hlls2.Add(hll2);

			List<Value.HLLValue> hlls3 = new List<Value.HLLValue>();
			hlls3.Add(hll3);

			policy.filterExp = Exp.Build(
				Exp.GE(
					HLLExp.GetSimilarity(Exp.Val(hlls2), Exp.HLLBin(bin1)),
					HLLExp.GetSimilarity(Exp.Val(hlls3), Exp.HLLBin(bin1))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.LE(
					HLLExp.GetSimilarity(Exp.Val(hlls2), Exp.HLLBin(bin1)),
					HLLExp.GetSimilarity(Exp.Val(hlls3), Exp.HLLBin(bin1))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Describe(Key key)
		{
			Exp index = Exp.Val(0);

			policy.filterExp = Exp.Build(
				Exp.NE(
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, index, HLLExp.Describe(Exp.HLLBin(bin1))),
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, index, HLLExp.Describe(Exp.HLLBin(bin2)))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, index, HLLExp.Describe(Exp.HLLBin(bin1))),
					ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, index, HLLExp.Describe(Exp.HLLBin(bin2)))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void MayContain(Key key)
		{
			List<Value> values = new List<Value>();
			values.Add(Value.Get("new_val"));

			policy.filterExp = Exp.Build(Exp.EQ(HLLExp.MayContain(Exp.Val(values), Exp.HLLBin(bin2)), Exp.Val(1)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(Exp.NE(HLLExp.MayContain(Exp.Val(values), Exp.HLLBin(bin2)), Exp.Val(1)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Add(Key key)
		{
			List<Value> values = new List<Value>();
			values.Add(Value.Get("new_val"));

			policy.filterExp = Exp.Build(
				Exp.EQ(
					HLLExp.GetCount(Exp.HLLBin(bin1)),
					HLLExp.GetCount(HLLExp.Add(HLLPolicy.Default, Exp.Val(values), Exp.HLLBin(bin2)))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.LT(
					HLLExp.GetCount(Exp.HLLBin(bin1)),
					HLLExp.GetCount(HLLExp.Add(HLLPolicy.Default, Exp.Val(values), Exp.HLLBin(bin2)))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}
	}
}
