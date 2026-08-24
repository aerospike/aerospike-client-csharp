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
	public class TestErrorDetailVerbosity : TestSync
	{
		private static readonly string binName = "edv-bin";
		private static Key intKey;
		private static Key strKey;
		private static Key listKey;

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "extended errors");
			WritePolicy wp = new();
			intKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-int-key");
			strKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-str-key");
			listKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-list-key");

			client.Put(wp, intKey, new Bin(binName, 1));
			client.Put(wp, strKey, new Bin(binName, "hello"));
			client.Put(wp, listKey, new Bin(binName, new List<int> { 10, 20, 30 }));
		}

		[TestMethod]
		public void TestDefaultVerbosityIsZero()
		{
			Policy p = new Policy();
			Assert.AreEqual(0, p.errorDetailVerbosity);

			WritePolicy wp = new();
			Assert.AreEqual(0, wp.errorDetailVerbosity);
		}

		[TestMethod]
		public void TestVerbosityDisabled()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 0
			};
			try
			{
				client.Operate(wp, intKey, Operation.Append(new Bin(binName, "bad")));
				Assert.Fail("Expected AerospikeException");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.BIN_TYPE_ERROR, ae.Result);
				// With verbosity 0, the message should be the default ResultCode string.
				Assert.AreEqual(ResultCode.GetResultString(ResultCode.BIN_TYPE_ERROR), ae.BaseMessage);
			}
		}

		[TestMethod]
		public void TestVerbositySubcodeOnly()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 1
			};
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-subonly-key");
			client.Put(new WritePolicy(), key, new Bin("other-bin", 1));

			try
			{
				client.Operate(wp, key, HLLOperation.RefreshCount("no-hll-bin"));
				Assert.Fail("Expected AerospikeException");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.BIN_NOT_FOUND, ae.Result);
				Assert.AreEqual(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.SubCode);
				Assert.IsTrue(ae.BaseMessage.Contains("subcode=1"));
			}
		}

		[TestMethod]
		public void TestVerbositySubcodeAndMessage()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-submsg-key");
			client.Put(new WritePolicy(), key, new Bin("other-bin", 1));

			try
			{
				client.Operate(wp, key, HLLOperation.RefreshCount("no-hll-bin"));
				Assert.Fail("Expected AerospikeException");
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.BIN_NOT_FOUND, ae.Result);
				Assert.AreEqual(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.SubCode);
				Assert.IsTrue(ae.BaseMessage.Contains("subcode=1"));
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestAppendToIntegerBin()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			try
			{
				client.Operate(wp, intKey, Operation.Append(new Bin(binName, "bad-append")));
				Assert.Fail("Expected AerospikeException");
			}
			catch (AerospikeException ae)
			{
				AssertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "cannot append");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestDeleteGenerationMismatch()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2,
				generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
				generation = 777
			};

			try
			{
				client.Delete(wp, intKey);
			}
			catch (AerospikeException ae)
			{
				AssertSubcodeAbsent(ae, ResultCode.GENERATION_ERROR, "generation");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestIncrementStringBin()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			try
			{
				client.Operate(wp, strKey, Operation.Add(new Bin(binName, 1)));
			}
			catch (AerospikeException ae)
			{
				AssertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "cannot increment");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestHllAddOnIntegerBin()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			List<Value> hllList = new List<Value>();
			hllList.Add(Value.Get("element1"));

			try
			{
				client.Operate(wp, intKey,
					HLLOperation.Add(HLLPolicy.Default, binName, hllList, 8));
			}
			catch (AerospikeException ae)
			{
				AssertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "bin is not hll type");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestHllRefreshCountMissingBin()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			Key key3 = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-no-hll-key");
			client.Put(new WritePolicy(), key3, new Bin("other-bin", 1));

			try
			{
				client.Operate(wp, key3, HLLOperation.RefreshCount("no-hll-bin"));
			}
			catch (AerospikeException ae)
			{
				// AS_SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP = 1
				AssertSubcode(ae, ResultCode.BIN_NOT_FOUND, SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestListGetIndexOutOfBounds()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			try
			{
				client.Operate(wp, listKey, ListOperation.Get(binName, 99));
			}
			catch (AerospikeException ae)
			{
				AssertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestListGetByRankOutOfBounds()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};

			try
			{
				client.Operate(wp, listKey, ListOperation.GetByRank(binName, 99, ListReturnType.VALUE));
			}
			catch (AerospikeException ae)
			{
				AssertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_RANK_OUT_OF_BOUNDS);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestListBoundedOverflow()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};
			ListPolicy bounded = new(ListOrder.ORDERED, ListWriteFlags.INSERT_BOUNDED);

			try
			{
				client.Operate(wp, listKey, ListOperation.Insert(bounded, binName, 10, Value.Get(5)));
			}
			catch (AerospikeException ae)
			{
				AssertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestBitGetOffsetOutOfRange()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-bits-key");
			client.Put(new WritePolicy(), key, new Bin(binName, new byte[] { 0xAA, 0xBB, 0xCC, 0xDD }));

			try
			{
				client.Operate(wp, key, BitOperation.Get(binName, 2000000000, 8));
			}
			catch (AerospikeException ae)
			{
				AssertSubcode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_OFFSET_OUT_OF_RANGE);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestReadFilteredOut()
		{
			Policy p = new()
			{
				errorDetailVerbosity = 2,
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binName), Exp.Val(99))),
				failOnFilteredOut = true
			};

			try
			{
				client.Get(p, intKey);
			}
			catch (AerospikeException ae)
			{
				AssertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestSuccessNoErrorDetails()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 2
			};
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-success-key");
			client.Put(wp, key, new Bin(binName, 42));
			Record record = client.Get(new Policy(), key);
			Assert.IsNotNull(record);
			Assert.AreEqual(42, record.GetInt(binName));
		}

		// ---------------------------------------------------------------------
		// Verbosity 3: expression build-failure trace (SERVER-1137).
		//
		// A type-mismatched comparison expression fails to build on the server.
		// As a filter expression it yields "invalid filter expression in request";
		// as an expression write operation it yields "invalid expression in operation
		// request". Both carry PARAMETER_ERROR + SubCode.NONE and, at verbosity 3,
		// a structured build trace. Assert trace presence and shape, not exact offsets.
		// ---------------------------------------------------------------------

		private static Exp BadExp()
		{
			return Exp.EQ(Exp.Val(5), Exp.Val(6.0));
		}

		[TestMethod]
		public void TestFilterExpBuildFailureTrace()
		{
			Policy p = new()
			{
				errorDetailVerbosity = 3,
				filterExp = Exp.Build(BadExp())
			};

			try
			{
				client.Get(p, intKey);
			}
			catch (AerospikeException ae)
			{
				AssertBuildTrace(ae, "invalid filter expression in request");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestExpWriteBuildFailureTrace()
		{
			WritePolicy wp = new()
			{
				errorDetailVerbosity = 3
			};

			try
			{
				client.Operate(wp, intKey, ExpOperation.Write(binName, Exp.Build(BadExp()), ExpWriteFlags.DEFAULT));
			}
			catch (AerospikeException ae)
			{
				AssertBuildTrace(ae, "invalid expression in operation request");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestFilterExpBuildFailureVerbosity2HasNoTrace()
		{
			Policy p = new()
			{
				errorDetailVerbosity = 2,
				filterExp = Exp.Build(BadExp())
			};

			try
			{
				client.Get(p, intKey);
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
				Assert.AreEqual(SubCode.NONE, ae.SubCode);

				string msg = ae.BaseMessage;
				Assert.IsNotNull(msg);
				Assert.IsTrue(msg.Contains("invalid filter expression in request"),
					"Expected filter-build message in: " + msg);
				Assert.IsNull(ae.ExpTrace, "Verbosity 2 must surface no expression trace");
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		/// <summary>
		/// Assert the server-supplied result code and subcode pair. The numeric
		/// subcode must be exposed first-class on <see cref="AerospikeException.SubCode"/>
		/// and still appear in the message for parity with the C client.
		/// </summary>
		private static void AssertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode)
		{
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(expectedSubcode, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);
			Assert.IsTrue(msg.Contains("subcode=" + expectedSubcode));
		}

		/// <summary>
		/// Assert that the server surfaced a contextual message but no subcode
		/// (<see cref="SubCode.NONE"/>). The "(subcode=...)" suffix must not appear.
		/// </summary>
		private static void AssertSubcodeAbsent(AerospikeException ae, int expectedResultCode, params string[] expectedSubstrings)
		{
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);

			foreach (string expected in expectedSubstrings)
			{
				Assert.IsTrue(msg.Contains(expected), "Expected '" + expected + "' in: " + msg);
			}
			Assert.IsFalse(msg.Contains("subcode="), "Expected NO subcode suffix in: " + msg);
		}

		private static void AssertBuildTrace(AerospikeException ae, string expectedSubstring)
		{
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);
			Assert.IsTrue(msg.Contains(expectedSubstring), "Expected '" + expectedSubstring + "' in: " + msg);

			ExpressionTrace trace = ae.ExpTrace;
			Assert.IsNotNull(trace, "Expected a non-null expression trace at verbosity 3");
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, trace.Phase);
		}
	}
}
