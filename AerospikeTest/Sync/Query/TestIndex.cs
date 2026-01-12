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

namespace Aerospike.Test
{
	[TestClass]
	public class TestIndex : TestSync
	{
		private const string indexName = "testindex";
		private const string binName = "testbin";

		[TestMethod]
		public void IndexCreateDrop()
		{
			Policy policy = new();
			policy.SetTimeout(0);

			IndexTask task;

			// Drop index if it already exists.
			try
			{
				task = client.DropIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName);
				task.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_NOTFOUND)
				{
					throw;
				}
			}

			task = client.CreateIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();

			task = client.DropIndex(policy, SuiteHelpers.ns, SuiteHelpers.set, indexName);
			task.Wait();

			// Ensure all nodes have dropped the index.
			Node[] nodes = [.. client.Nodes];

			foreach (Node node in nodes)
			{
				string cmd = IndexTask.BuildStatusCommand(SuiteHelpers.ns, indexName, node.serverVersion);
				string response = Info.Request(node, cmd);
				int code = Info.ParseResultCode(response);
				Assert.AreEqual(201, code);
			}
		}

		[TestMethod]
		public void CtxRestore()
		{
			CTX[] ctx1 =
			[
				CTX.ListIndex(-1),
				CTX.MapKey(Value.Get("key1")),
				CTX.ListValue(Value.Get(937))
			];

			string base64 = CTX.ToBase64(ctx1);
			CTX[] ctx2 = CTX.FromBase64(base64);

			Assert.AreEqual(ctx1.Length, ctx2.Length);

			for (int i = 0; i < ctx1.Length; i++)
			{
				CTX item1 = ctx1[i];
				CTX item2 = ctx2[i];

				Assert.AreEqual(item1.id, item2.id);

				object obj1 = item1.value.Object;
				object obj2 = item2.value.Object;

				if (obj1 is int v && obj2 is long v1)
				{
					// FromBase64() converts integers to long, so consider these equivalent.
					Assert.AreEqual((long)v, v1);
				}
				else
				{
					Assert.AreEqual(obj1, obj2);
				}
			}
		}

		[TestMethod]
		public void AllChildrenBase()
		{
			CTX[] ctx1 =
			[
				CTX.AllChildren()
			];

			string base64 = CTX.ToBase64(ctx1);
			CTX[] ctx2 = CTX.FromBase64(base64);

			Assert.AreEqual(ctx1.Length, ctx2.Length);

			CTX original = ctx1[0];
			CTX restored = ctx2[0];

			Assert.AreEqual(original.id, restored.id);
			Assert.AreEqual(original.exp.Bytes.Length, restored.exp.Bytes.Length);

			// Verify the expression bytes are equivalent
			byte[] originalBytes = original.exp.Bytes;
			byte[] restoredBytes = restored.exp.Bytes;
			for (int i = 0; i < originalBytes.Length; i++)
			{
				Assert.AreEqual(originalBytes[i], restoredBytes[i]);
			}
		}

		[TestMethod]
		public void AllChildrenWithFilterBase()
		{
			Exp filter1 = Exp.GT(Exp.MapLoopVar(LoopVarPart.VALUE), Exp.Val(10));
			CTX[] ctxOne =
			[
				CTX.AllChildrenWithFilter(filter1)
			];

			string base64StringOne = CTX.ToBase64(ctxOne);
			CTX[] restoredContextOne = CTX.FromBase64(base64StringOne);

			Assert.AreEqual(ctxOne.Length, restoredContextOne.Length);
			Assert.AreEqual(ctxOne[0].id, restoredContextOne[0].id);
			Expression expression = ctxOne[0].exp;
			Expression restoredExpression = restoredContextOne[0].exp;
			Assert.AreEqual(expression.Bytes.Length, restoredExpression.Bytes.Length);

			// Test 2: String key filter
			Exp filterTwo = Exp.EQ(Exp.MapLoopVar(LoopVarPart.MAP_KEY), Exp.Val("target_key"));
			CTX[] contextTwo = [
				CTX.AllChildrenWithFilter(filterTwo)
			];

			string base64StringTwo = CTX.ToBase64(contextTwo);
			CTX[] restoredContextTwo = CTX.FromBase64(base64StringTwo);

			Assert.AreEqual(contextTwo.Length, restoredContextTwo.Length);
			Assert.AreEqual(contextTwo[0].id, restoredContextTwo[0].id);

			Expression expressionTwo = contextTwo[0].exp;
			Expression restoredExpressionTwo = restoredContextTwo[0].exp;
			Assert.AreEqual(expressionTwo.Bytes.Length, restoredExpressionTwo.Bytes.Length);

			// Test 3: Complex filter with AND/OR operations
			Exp filterThree = Exp.And(
				Exp.GT(Exp.MapLoopVar(LoopVarPart.VALUE), Exp.Val(5)),
				Exp.LT(Exp.MapLoopVar(LoopVarPart.VALUE), Exp.Val(50))
			);
			CTX[] contextThree =
			[
				CTX.AllChildrenWithFilter(filterThree)
			];

			string base64StringThree = CTX.ToBase64(contextThree);
			CTX[] restoredContextThree = CTX.FromBase64(base64StringThree);

			Assert.AreEqual(contextThree.Length, restoredContextThree.Length);
			Assert.AreEqual(contextThree[0].id, restoredContextThree[0].id);

			Expression expressionThree = contextThree[0].exp;
			Expression restoredExpressionThree = restoredContextThree[0].exp;

			Assert.AreEqual(expressionThree.Bytes.Length, restoredExpressionThree.Bytes.Length);

			// Test 4: Complex nested CTX with allChildrenWithFilter
			Exp filterFour = Exp.Or(
				Exp.EQ(Exp.MapLoopVar(LoopVarPart.MAP_KEY), Exp.Val("key1")),
				Exp.EQ(Exp.MapLoopVar(LoopVarPart.MAP_KEY), Exp.Val("key2"))
			);
			CTX[] contextFour = [
				CTX.MapKey(Value.Get("parent")),
				CTX.AllChildrenWithFilter(filterFour)
			];

			string base64StringFour = CTX.ToBase64(contextFour);
			CTX[] restoredContextFour = CTX.FromBase64(base64StringFour);

			Assert.AreEqual(contextFour.Length, restoredContextFour.Length);

			for (int i = 0; i < contextFour.Length; i++)
			{
				Assert.AreEqual(contextFour[i].id, restoredContextFour[i].id);

				if (contextFour[i].id == 0x04)
				{ // Exp.CTX_EXP - expression-based CTX
					Expression originalExpr = contextFour[i].exp;
					Expression restoredExpr = restoredContextFour[i].exp;

					Assert.AreEqual(originalExpr.Bytes.Length, restoredExpr.Bytes.Length);

					byte[] originalBytes = originalExpr.Bytes;
					byte[] restoredBytes = restoredExpr.Bytes;
					for (int j = 0; j < originalBytes.Length; j++)
					{
						Assert.AreEqual(originalBytes[j], restoredBytes[j]);
					}
				}
				else if (contextFour[i].value != null)
				{
					object objectOne = contextFour[i].value.Object;
					object objectTwo = restoredContextFour[i].value.Object;
					Assert.AreEqual(objectOne, objectTwo);
				}
			}
		}

		[TestMethod]
		public void MixedContextWithAllChildrenBase()
		{
			CTX[] ctx1 = [
				CTX.MapKey(Value.Get("root")),
				CTX.AllChildren(),
				CTX.ListIndex(-1),
				CTX.AllChildrenWithFilter(Exp.GT(Exp.MapLoopVar(LoopVarPart.VALUE), Exp.Val(0))),
				CTX.MapValue(Value.Get("test"))
			];

			string base64 = CTX.ToBase64(ctx1);
			CTX[] ctx2 = CTX.FromBase64(base64);

			Assert.AreEqual(ctx1.Length, ctx2.Length);

			for (int i = 0; i < ctx1.Length; i++)
			{
				CTX itemOne = ctx1[i];
				CTX itemTwo = ctx2[i];

				Assert.AreEqual(itemOne.id, itemTwo.id);

				if (itemOne.id == 0x04)
				{   // Exp.CTX_EXP - expression-based CTX
					Expression originalExpr = itemOne.exp;
					Expression restoredExpr = itemTwo.exp;

					Assert.AreEqual(originalExpr.Bytes.Length, restoredExpr.Bytes.Length);

					byte[] originalBytes = originalExpr.Bytes;
					byte[] restoredBytes = restoredExpr.Bytes;
					for (int j = 0; j < originalBytes.Length; j++)
					{
						Assert.AreEqual(originalBytes[j], restoredBytes[j]);
					}
				}
				else if (itemOne.value != null)
				{
					object objectOne = itemOne.value.Object;
					object objectTwo = itemTwo.value.Object;

					if (objectOne is int o1int && objectTwo is long o2long)
					{
						Assert.AreEqual((long)o1int, o2long);
					}
					else
					{
						Assert.AreEqual(objectOne, objectTwo);
					}
				}
			}
		}
	}
}
