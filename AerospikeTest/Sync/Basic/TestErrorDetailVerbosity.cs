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
			//CheckServerVersion(Node.SERVER_VERSION_8_1_3, "extended errors");
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
			Assert.AreEqual(0, p.ErrorDetailVerbosity);

			WritePolicy wp = new();
			Assert.AreEqual(0, wp.ErrorDetailVerbosity);
		}

		[TestMethod]
		public void TestVerbosityDisabled()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 0
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
				ErrorDetailVerbosity = 1
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2,
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2
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
				ErrorDetailVerbosity = 2,
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin(binName), Exp.Val(99))),
				failOnFilteredOut = true
			};

			try
			{
				client.Get(p, intKey);
			}
			catch (AerospikeException ae)
			{
				AssertSubcode(ae, ResultCode.FILTERED_OUT, SubCode.FILTERED_BINS);
				return;
			}
			Assert.Fail("Expected AerospikeException");
		}

		[TestMethod]
		public void TestSuccessNoErrorDetails()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 2
			};
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-success-key");
			client.Put(wp, key, new Bin(binName, 42));
			Record record = client.Get(new Policy(), key);
			Assert.IsNotNull(record);
			Assert.AreEqual(42, record.GetInt(binName));
		}

		/**
		* Assert the server-supplied {@code (resultCode, subcode)} pair. The numeric
		* subcode must be exposed first-class via {@link AerospikeException#getSubcode()}
		* (not merely embedded in the message string), and the "subcode=N" suffix must
		* still appear in the message for parity with the C client.
		*/
		private static void AssertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode)
		{
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(expectedSubcode, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);
			Assert.IsTrue(msg.Contains("subcode=" + expectedSubcode));
		}

		/**
		* Assert that the server surfaced a contextual message but NO subcode
		* (AS_SUB_NONE): {@link AerospikeException#getSubcode()} is {@link SubCode#NONE}
		* and the "(subcode=...)" suffix must never appear. Any expectedSubstrings are
		* required in the message; pass none to skip the message-text check (mirrors a
		* NULL expected_msg_substr in the C example).
		*/
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
	}
}
