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
using Aerospike.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestListExp : TestSync
	{
		private readonly bool InstanceFieldsInitialized = false;

		public TestListExp()
		{
			if (!InstanceFieldsInitialized)
			{
				InitializeInstanceFields();
				InstanceFieldsInitialized = true;
			}
		}

		private void InitializeInstanceFields()
		{
			keyA = new Key(SuiteHelpers.ns, SuiteHelpers.set, binA);
			keyB = new Key(SuiteHelpers.ns, SuiteHelpers.set, binB);
		}

		private readonly string binA = "A";
		private readonly string binB = "B";
		private readonly string binC = "C";

		private Key keyA;
		private Key keyB;

		private Policy policy;

		[TestInitialize()]
		public void SetUp()
		{
			client.Delete(null, keyA);
			client.Delete(null, keyB);
			policy = new Policy();
		}

		[TestMethod]
		public void ModifyWithContext()
		{
			IList<Value> listSubA =
			[
				Value.Get("e"),
				Value.Get("d"),
				Value.Get("c"),
				Value.Get("b"),
				Value.Get("a"),
			];

			IList<Value> listA =
			[
				Value.Get("a"),
				Value.Get("b"),
				Value.Get("c"),
				Value.Get("d"),
				Value.Get(listSubA),
			];

			IList<Value> listB = [Value.Get("x"), Value.Get("y"), Value.Get("z")];

			client.Operate(null, keyA,
				ListOperation.AppendItems(ListPolicy.Default, binA, (IList)listA),
				ListOperation.AppendItems(ListPolicy.Default, binB, (IList)listB),
				Operation.Put(new Bin(binC, "M"))
				);

			CTX ctx = CTX.ListIndex(4);
			Record record;
			IList result;

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size(
						// Temporarily Append binB/binC to binA in expression.
						ListExp.AppendItems(ListPolicy.Default, Exp.ListBin(binB),
							ListExp.Append(ListPolicy.Default, Exp.StringBin(binC), Exp.ListBin(binA), ctx),
							ctx),
						ctx),
					Exp.Val(9)));

			record = client.Get(policy, keyA, binA);
			AssertRecordFound(keyA, record);

			result = record.GetList(binA);
			Assert.AreEqual(5, result.Count);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					ListExp.Size(
						// Temporarily Append local listB and local "M" string to binA in expression.
						ListExp.AppendItems(ListPolicy.Default, Exp.Val((IList)listB),
							ListExp.Append(ListPolicy.Default, Exp.Val("M"), Exp.ListBin(binA), ctx),
							ctx),
						ctx),
					Exp.Val(9)));

			record = client.Get(policy, keyA, binA);
			AssertRecordFound(keyA, record);

			result = record.GetList(binA);
			Assert.AreEqual(5, result.Count);
		}

		[TestMethod]
		public void ExpReturnsList()
		{
			List<Value> list = [Value.Get("a"), Value.Get("b"), Value.Get("c"), Value.Get("d")];

			Expression exp = Exp.Build(Exp.Val(list));

			Record record = client.Operate(null, keyA,
				ExpOperation.Write(binC, exp, ExpWriteFlags.DEFAULT),
				Operation.Get(binC),
				ExpOperation.Read("var", exp, ExpReadFlags.DEFAULT)
				);

			IList results = record.GetList(binC);
			Assert.AreEqual(2, results.Count);

			IList rlist = (IList)results[1];
			Assert.AreEqual(4, rlist.Count);

			IList results2 = record.GetList("var");
			Assert.AreEqual(4, results2.Count);
		}
	}
}
