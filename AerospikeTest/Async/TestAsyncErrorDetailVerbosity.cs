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
	public class TestAsyncErrorDetailVerbosity : TestAsync
	{
		private static readonly string binName = "edv-bin";
		private static Key intKey;
		private static Key listKey;

		[ClassInitialize()]
		public static void Setup(TestContext testContext)
		{
			//CheckServerVersion(new Version(8, 1, 3, 0), "extended errors");

			WritePolicy wp = new();
			intKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-int-key");
			SuiteHelpers.client.Put(wp, intKey, new Bin(binName, 1));
			listKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "edv-list-key");
			SuiteHelpers.client.Put(wp, listKey, new Bin(binName, new List<int> { 10 }));
		}

		// AsyncOperateWrite — type mismatch surfaces subcode + message
		[TestMethod]
		public void AsyncOperateWriteSurfacesDetail()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 2
			};
			ListPolicy bounded = new(ListOrder.ORDERED, ListWriteFlags.INSERT_BOUNDED);

			client.Operate(wp, new OperateWriteHandler(this), listKey, ListOperation.Insert(bounded, binName, 10, Value.Get(5)));

			WaitTillComplete();
		}

		private class OperateWriteHandler(TestAsyncErrorDetailVerbosity parent) : RecordListener
		{
			public void OnSuccess(Key key, Record record)
			{
				parent.SetError(new Exception("Expected OP_NOT_APPLICABLE, got success"));
				parent.NotifyCompleted();
			}
			public void OnFailure(AerospikeException e)
			{
				AssertSubcode(e, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW);
				parent.NotifyCompleted();
			}
		}

		// AsyncDelete — generation mismatch surfaces subcode + message
		[TestMethod]
		public void AsyncDeleteSurfacesDetail()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 2,
				generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
				generation = 777
			};

			client.Delete(wp, new DeleteListenerHandler(this), intKey);

			WaitTillComplete();
		}

		private class DeleteListenerHandler(TestAsyncErrorDetailVerbosity parent) : DeleteListener
		{
			public void OnSuccess(Key key, bool existed)
			{
				parent.SetError(new Exception("Expected GENERATION_ERROR, got success"));
				parent.NotifyCompleted();
			}
			public void OnFailure(AerospikeException e)
			{
				AssertSubcodeAbsent(e, ResultCode.GENERATION_ERROR, "generation mismatch");
				parent.NotifyCompleted();
			}
		}

		// AsyncWrite — generation mismatch surfaces subcode + message
		[TestMethod]
		public void AsyncWriteSurfacesDetail()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 2,
				generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
				generation = 777
			};

			client.Put(wp, new WriteListenerHandler(this), intKey, new Bin(binName, 2));

			WaitTillComplete();
		}

		private class WriteListenerHandler(TestAsyncErrorDetailVerbosity parent) : WriteListener
		{
			public void OnSuccess(Key key)
			{
				parent.SetError(new Exception("Expected GENERATION_ERROR, got success"));
				parent.NotifyCompleted();
			}
			public void OnFailure(AerospikeException e)
			{
				AssertSubcodeAbsent(e, ResultCode.GENERATION_ERROR, "generation mismatch");
				parent.NotifyCompleted();
			}
		}

		// AsyncTouch — generation mismatch surfaces subcode + message
		[TestMethod]
		public void AsyncTouchSurfacesDetail()
		{
			WritePolicy wp = new()
			{
				ErrorDetailVerbosity = 2,
				generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
				generation = 777
			};

			client.Touch(wp, new TouchListenerHandler(this), intKey);

			WaitTillComplete();
		}

		private class TouchListenerHandler(TestAsyncErrorDetailVerbosity parent) : WriteListener
		{
			public void OnSuccess(Key key)
			{
				parent.SetError(new Exception("Expected GENERATION_ERROR, got success"));
				parent.NotifyCompleted();
			}
			public void OnFailure(AerospikeException e)
			{
				AssertSubcodeAbsent(e, ResultCode.GENERATION_ERROR, "generation mismatch");
				parent.NotifyCompleted();
			}
		}

		// AsyncExists — uses Policy (not WritePolicy). Server should not error on plain exists;
		// just verifies the configured verbosity does not break the happy path.
		[TestMethod]
		public void AsyncExistsVerbositySetHappyPath()
		{
			Policy p = new()
			{
				ErrorDetailVerbosity = 2
			};

			client.Exists(p, new ExistsListenerHandler(this), intKey);

			WaitTillComplete();
		}

		private class ExistsListenerHandler(TestAsyncErrorDetailVerbosity parent) : ExistsListener
		{
			public void OnSuccess(Key key, bool exists)
			{
				if (!exists)
				{
					parent.SetError(new Exception("Expected record to exist"));
				}
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		// AsyncRead — verifies happy path with verbosity set
		[TestMethod]
		public void AsyncReadVerbositySetHappyPath()
		{
			Policy p = new()
			{
				ErrorDetailVerbosity = 2
			};

			client.Get(p, new ReadVerbosityHandler(this), intKey);
			WaitTillComplete();
		}

		private class ReadVerbosityHandler(TestAsyncErrorDetailVerbosity parent) : RecordListener
		{
			public void OnSuccess(Key key, Record record)
			{
				if (record == null || record.GetInt(binName) != 1)
				{
					parent.SetError(new Exception("Unexpected record: " + record));
				}
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		// AsyncReadHeader — verifies happy path with verbosity set
		[TestMethod]
		public void AsyncReadHeaderVerbositySetHappyPath()
		{
			Policy p = new()
			{
				ErrorDetailVerbosity = 2
			};

			client.GetHeader(p, new ReadHeaderVerbosityHandler(this), intKey);
			WaitTillComplete();
		}

		private class ReadHeaderVerbosityHandler(TestAsyncErrorDetailVerbosity parent) : RecordListener
		{
			public void OnSuccess(Key key, Record record)
			{
				if (record == null)
				{
					parent.SetError(new Exception("Expected header"));
				}
				parent.NotifyCompleted();
			}
			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		/// <summary>
		/// Assert the server-supplied (resultCode, subcode) pair reached the
		/// async exception, including the first-class numeric subcode.
		/// </summary>
		/// <param name="ae">The AerospikeException to check.</param>
		/// <param name="expectedResultCode">The expected result code.</param>
		/// <param name="expectedSubcode">The expected subcode.</param>
		private static void AssertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode)
		{
			Assert.IsNotNull(ae);
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(expectedSubcode, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);
			Assert.IsTrue(msg.Contains("subcode=" + expectedSubcode));
		}

		/// <summary>
		/// Assert that the server surfaced a contextual message but NO subcode
		/// (AS_SUB_NONE): AerospikeException.SubCode is SubCode.NONE
		/// and the "(subcode=...)" suffix must never appear.
		/// </summary>
		/// <param name="ae">The AerospikeException to check.</param>
		/// <param name="expectedResultCode">The expected result code.</param>
		/// <param name="expectedSubstring">The expected substring.</param>
		private static void AssertSubcodeAbsent(AerospikeException ae, int expectedResultCode, String expectedSubstring)
		{
			Assert.IsNotNull(ae);
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);

			string msg = ae.BaseMessage;
			Assert.IsNotNull(msg, "Expected server error message, got null. ae=" + ae);
			Assert.IsTrue(msg.Contains(expectedSubstring));
			Assert.IsFalse(msg.Contains("subcode="));
		}
	}
}