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
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestExpErrorDetail : TestSync
	{
		private const string WriteOnlyUser = "eed_errdetail_wuser";
		private const string WriteOnlyPassword = "eed_errdetail_wpwd";

		private const string BIN_INT = "x";
		private const string BIN_FLOAT = "y";
		private const string BIN_STR = "name";
		private const string BIN_LIST = "xs";
		private const string BIN_MAP1 = "um1";
		private const string BIN_MAP2 = "um2";
		private const string BIN_MISSING = "missing";
		private const string SCRATCH_KEEP_BIN = "keep";

		private static readonly string[] parityVerbs = ["put", "delete", "operate"];

		private static Key stdKey;
		private static Key scratchKey;
		private static bool writeOnlyReady;
		private static bool writeOnlyUserCreated;
		private static AerospikeClient writeOnlyClient;

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "extended errors");

			stdKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "eed-std-key");
			scratchKey = new Key(SuiteHelpers.ns, SuiteHelpers.set, "eed-scratch-key");

			client.Put(new WritePolicy(), stdKey,
				new Bin(BIN_INT, 10),
				new Bin(BIN_FLOAT, 2.5),
				new Bin(BIN_STR, "ael"),
				new Bin(BIN_LIST, new List<int> { 1, 2, 3 }),
				new Bin(BIN_MAP1, new Dictionary<string, int> { { "a", 1 } }),
				new Bin(BIN_MAP2, new Dictionary<string, int> { { "b", 2 } }));

			ReseedScratch();
			RegisterUdf();
			writeOnlyReady = TrySetupWriteOnlyClient();
		}

		[ClassCleanup]
		public static void Cleanup()
		{
			TeardownWriteOnlyClient();
		}

		private static void RegisterUdf()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua",
				"record_example.lua", Language.LUA);
			task.Wait();
		}

		private static bool TrySetupWriteOnlyClient()
		{
			writeOnlyUserCreated = false;

			if (string.IsNullOrEmpty(SuiteHelpers.user) || !CurrentUserCanProvisionWriteOnlyPrincipal())
			{
				return false;
			}

			AdminPolicy adminPolicy = new();

			try
			{
				try
				{
					client.DropUser(adminPolicy, WriteOnlyUser);
				}
				catch (AerospikeException)
				{
				}

				client.CreateUser(adminPolicy, WriteOnlyUser, WriteOnlyPassword, [Role.Write]);
				writeOnlyUserCreated = true;

				writeOnlyClient?.Close();
				writeOnlyClient = CreateWriteOnlyClient();
				return true;
			}
			catch (AerospikeException)
			{
				writeOnlyClient?.Close();
				writeOnlyClient = null;

				if (writeOnlyUserCreated)
				{
					try
					{
						client.DropUser(adminPolicy, WriteOnlyUser);
					}
					catch (AerospikeException)
					{
					}

					writeOnlyUserCreated = false;
				}

				return false;
			}
		}

		/// <summary>
		/// The write-only explainer test provisions a temporary user and needs
		/// <see cref="Role.UserAdmin"/> on the runsettings test account.
		/// </summary>
		private static bool CurrentUserCanProvisionWriteOnlyPrincipal()
		{
			AdminPolicy adminPolicy = new();

			try
			{
				User user = client.QueryUser(adminPolicy, SuiteHelpers.user);
				return user.roles != null && user.roles.Contains(Role.UserAdmin);
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.SECURITY_NOT_ENABLED ||
					e.Result == ResultCode.SECURITY_NOT_SUPPORTED)
				{
					return false;
				}

				return false;
			}
		}

		private static AerospikeClient CreateWriteOnlyClient()
		{
			ClientPolicy policy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate,
				user = WriteOnlyUser,
				password = WriteOnlyPassword
			};

			return new AerospikeClient(policy, SuiteHelpers.hosts);
		}

		private static void TeardownWriteOnlyClient()
		{
			writeOnlyClient?.Close();
			writeOnlyClient = null;

			if (!writeOnlyUserCreated || string.IsNullOrEmpty(SuiteHelpers.user))
			{
				return;
			}

			AdminPolicy adminPolicy = new();

			try
			{
				client.DropUser(adminPolicy, WriteOnlyUser);
			}
			catch (AerospikeException)
			{
			}

			writeOnlyUserCreated = false;
		}

		private static void ReseedScratch()
		{
			client.Put(new WritePolicy(), scratchKey, new Bin(BIN_INT, 10), new Bin(SCRATCH_KEEP_BIN, 1));
		}

		private static Expression BuildErrorExp()
		{
			return Exp.Build(Exp.EQ(Exp.Val(5), Exp.Val(6.0)));
		}

		private static Expression DivZeroFilterExp()
		{
			return Exp.Build(Exp.GT(Exp.Div(Exp.Val(5), Exp.Val(0)), Exp.Val(1)));
		}

		private static Exp CdtOobExp()
		{
			return ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(9), Exp.ListBin(BIN_LIST));
		}

		private static AerospikeException ExpectFilteredGet(int verbosity, Expression filter, int expectedResultCode)
		{
			Policy policy = new()
			{
				errorDetailVerbosity = verbosity,
				filterExp = filter,
				failOnFilteredOut = true
			};

			try
			{
				client.Get(policy, stdKey);
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(expectedResultCode, ae.Result);
				return ae;
			}

			Assert.Fail("Expected AerospikeException with result code " + expectedResultCode);
			return null;
		}

		private static AerospikeException ExpectOperateError(Key key, int verbosity, int expectedResultCode, Operation operation)
		{
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = verbosity
			};

			try
			{
				client.Operate(writePolicy, key, operation);
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(expectedResultCode, ae.Result);
				return ae;
			}

			Assert.Fail("Expected AerospikeException with result code " + expectedResultCode);
			return null;
		}

		private static WritePolicy FilterPolicy(int verbosity, Expression filter)
		{
			return new WritePolicy()
			{
				errorDetailVerbosity = verbosity,
				filterExp = filter,
				failOnFilteredOut = true
			};
		}

		private static AerospikeException ExpectVerbError(string verb, WritePolicy policy, int expectedResultCode)
		{
			AerospikeException exception = RunVerb(verb, policy);

			if (exception == null)
			{
				Assert.Fail("Expected AerospikeException with result code " + expectedResultCode + " for " + verb);
			}

			Assert.AreEqual(expectedResultCode, exception.Result, "Unexpected result code for " + verb);
			return exception;
		}

		private static AerospikeException RunVerb(string verb, WritePolicy policy)
		{
			ReseedScratch();

			try
			{
				switch (verb)
				{
					case "put":
						client.Put(policy, scratchKey, new Bin(BIN_INT, 11));
						break;

					case "delete":
						client.Delete(policy, scratchKey);
						break;

					case "operate":
						client.Operate(policy, scratchKey, Operation.Put(new Bin(BIN_INT, 11)));
						break;

					default:
						Assert.Fail("Unexpected verb: " + verb);
						break;
				}
			}
			catch (AerospikeException ae)
			{
				return ae;
			}

			return null;
		}

		private static ExpressionTrace AssertEvalTrace(AerospikeException ae, string op, int depth, string[] path)
		{
			ExpressionTrace trace = ae.ExpTrace;
			Assert.IsNotNull(trace, "Expected a non-null expression trace at verbosity 3");
			Assert.AreEqual(ExpressionTrace.PHASE_EVAL, trace.Phase);
			Assert.AreEqual(op, trace.Op);
			Assert.AreEqual(depth, trace.Depth);
			CollectionAssert.AreEqual(path, trace.Path);
			Assert.AreEqual(-1, trace.ByteOffset, "Runtime traces must not carry byte_offset");
			Assert.IsNotNull(trace.Snippet, "Expected an op-stream snippet");
			return trace;
		}

		private static ExpressionTrace AssertBuildTrace(AerospikeException ae)
		{
			ExpressionTrace trace = ae.ExpTrace;
			Assert.IsNotNull(trace, "Expected a non-null expression trace at verbosity 3");
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, trace.Phase);
			Assert.IsTrue(trace.ByteOffset >= 0, "Msgpack build traces must carry byte_offset");
			return trace;
		}

		private static void AssertMessageContains(AerospikeException ae, string expected)
		{
			string message = ae.BaseMessage;
			Assert.IsNotNull(message, "Expected server error message");
			Assert.IsTrue(message.Contains(expected), "Expected '" + expected + "' in: " + message);
		}

		private static void AssertMessageContainsAny(AerospikeException ae, params string[] expected)
		{
			string message = ae.BaseMessage;
			Assert.IsNotNull(message, "Expected server error message");

			foreach (string text in expected)
			{
				if (message.Contains(text))
				{
					return;
				}
			}

			Assert.Fail("Expected one of '" + string.Join("', '", expected) + "' in: " + message);
		}

		private static void AssertNoDetails(AerospikeException ae, int expectedResultCode)
		{
			Assert.AreEqual(expectedResultCode, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			Assert.AreEqual(ResultCode.GetResultString(expectedResultCode), ae.BaseMessage);
			Assert.IsNull(ae.ExpTrace, "Expected no expression trace");
		}

		[TestMethod]
		public void TestFilterFaultDivByZeroTrace()
		{
			AerospikeException ae = ExpectFilteredGet(3, DivZeroFilterExp(), ResultCode.FILTERED_OUT);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "integer division by zero");

			ExpressionTrace trace = AssertEvalTrace(ae, "div", 2, ["gt", "div"]);
			Assert.IsTrue(trace.Snippet.Contains("div("), "Expected div op in snippet: " + trace.Snippet);
		}

		[TestMethod]
		public void TestFilterFaultModByZeroTrace()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.Mod(Exp.Val(5), Exp.Val(0)), Exp.Val(1)));
			AerospikeException ae = ExpectFilteredGet(3, expression, ResultCode.FILTERED_OUT);

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "integer modulo by zero");
			AssertEvalTrace(ae, "mod", 2, ["eq", "mod"]);
		}

		[TestMethod]
		public void TestFilterFaultCdtOutOfBoundsSubcodeTrace()
		{
			Expression expression = Exp.Build(Exp.EQ(CdtOobExp(), Exp.Val(1)));
			AerospikeException ae = ExpectFilteredGet(3, expression, ResultCode.FILTERED_OUT);

			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.SubCode);
			AssertMessageContains(ae, "out of bounds");
			AssertEvalTrace(ae, "call", 2, ["eq", "call"]);
		}

		[TestMethod]
		public void TestFilterFaultUnorderedMapCompareTrace()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.MapBin(BIN_MAP1), Exp.MapBin(BIN_MAP2)));
			AerospikeException ae = ExpectFilteredGet(3, expression, ResultCode.FILTERED_OUT);

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContainsAny(ae, "ordering not defined", "cannot compare an unordered map");
			AssertEvalTrace(ae, "eq", 1, ["eq"]);
		}

		[TestMethod]
		public void TestVerbosity1NoDetailsForSubNoneFault()
		{
			AerospikeException ae = ExpectFilteredGet(1, DivZeroFilterExp(), ResultCode.FILTERED_OUT);
			AssertNoDetails(ae, ResultCode.FILTERED_OUT);
		}

		[TestMethod]
		public void TestVerbosity1CdtSubcodeOnly()
		{
			Expression expression = Exp.Build(Exp.EQ(CdtOobExp(), Exp.Val(1)));
			AerospikeException ae = ExpectFilteredGet(1, expression, ResultCode.FILTERED_OUT);

			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.SubCode);
			Assert.IsTrue(ae.Message.StartsWith("Error " + ResultCode.FILTERED_OUT + "," + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS),
				"Expected subcode in Message prefix. ae=" + ae);
			Assert.IsFalse(ae.Message.Contains("out of bounds"), "Tier 1 must surface no message text in: " + ae.Message);
			Assert.IsNull(ae.ExpTrace, "Tier 1 must surface no trace");
		}

		[TestMethod]
		public void TestVerbosity2NoTraceForRuntimeFault()
		{
			AerospikeException ae = ExpectFilteredGet(2, DivZeroFilterExp(), ResultCode.FILTERED_OUT);

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "integer division by zero");
			Assert.IsNull(ae.ExpTrace, "Verbosity 2 must surface no expression trace");
		}

		[TestMethod]
		public void TestParityBuildFailureTrace()
		{
			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(3, BuildErrorExp()), ResultCode.PARAMETER_ERROR);

				Assert.AreEqual(SubCode.NONE, ae.SubCode, verb);
				AssertMessageContains(ae, "invalid filter expression in request");
				AssertBuildTrace(ae);
			}
		}

		[TestMethod]
		public void TestParityRuntimeFaultTrace()
		{
			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(3, DivZeroFilterExp()), ResultCode.FILTERED_OUT);

				Assert.AreEqual(SubCode.NONE, ae.SubCode, verb);
				AssertMessageContains(ae, "integer division by zero");
				AssertEvalTrace(ae, "div", 2, ["gt", "div"]);
			}
		}

		[TestMethod]
		public void TestParityFalseFilter()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.IntBin(BIN_INT), Exp.Val(11)));

			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(3, expression), ResultCode.FILTERED_OUT);

				Assert.AreEqual(SubCode.NONE, ae.SubCode, verb);
				AssertMessageContains(ae, "filtered out");
			}
		}

		[TestMethod]
		public void TestParityAbsentBinTrace()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.IntBin(BIN_MISSING), Exp.Val(2)));

			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(3, expression), ResultCode.FILTERED_OUT);

				Assert.AreEqual(SubCode.NONE, ae.SubCode, verb);
				AssertMessageContainsAny(ae,
					"expression references an absent bin or key",
					"filter references an absent bin or key");
				ExpressionTrace trace = AssertEvalTrace(ae, "bin", 2, ["eq", "bin"]);
				Assert.AreEqual(ExpressionTrace.PHASE_EVAL, trace.Phase);
			}
		}

		[TestMethod]
		public void TestParityMetadataFalseVerbSpecificMessage()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.TTL(), Exp.Val(-5)));
			Dictionary<string, string> expected = new()
			{
				{ "put", "write filtered out by metadata filter" },
				{ "delete", "delete filtered out by metadata filter" },
				{ "operate", "write filtered out by metadata filter" }
			};

			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(3, expression), ResultCode.FILTERED_OUT);

				AssertMessageContains(ae, expected[verb]);
				Assert.IsNull(ae.ExpTrace, "Metadata-phase false must stage no trace for " + verb);
			}
		}

		[TestMethod]
		public void TestParityTier2MessageNoTrace()
		{
			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = ExpectVerbError(verb,
					FilterPolicy(2, DivZeroFilterExp()), ResultCode.FILTERED_OUT);

				AssertMessageContains(ae, "integer division by zero");
				Assert.IsNull(ae.ExpTrace, "Verbosity 2 must surface no trace for " + verb);
			}
		}

		[TestMethod]
		public void TestParityCleanPass()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.IntBin(BIN_INT), Exp.Val(10)));

			foreach (string verb in parityVerbs)
			{
				AerospikeException ae = RunVerb(verb, FilterPolicy(3, expression));
				Assert.IsNull(ae, "Expected success for " + verb + ", got: " + ae);
			}
		}

		[TestMethod]
		public void TestExpReadBuildFailureTrace()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.PARAMETER_ERROR,
				ExpOperation.Read("result", BuildErrorExp(), ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "invalid expression in operation request");
			AssertBuildTrace(ae);
		}

		[TestMethod]
		public void TestExpReadNonBoolRootLegal()
		{
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			Record record = client.Operate(writePolicy, stdKey,
				ExpOperation.Read("result", Exp.Build(Exp.Add(Exp.IntBin(BIN_INT), Exp.Val(1))),
					ExpReadFlags.DEFAULT));

			Assert.IsNotNull(record);
			Assert.AreEqual(11, record.GetInt("result"));
		}

		[TestMethod]
		public void TestExpReadInvalidFlagsNoDetails()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.PARAMETER_ERROR,
				ExpOperation.Read("result", Exp.Build(Exp.Add(Exp.IntBin(BIN_INT), Exp.Val(1))),
					(ExpReadFlags)(int)ExpWriteFlags.CREATE_ONLY));

			AssertNoDetails(ae, ResultCode.PARAMETER_ERROR);
		}

		[TestMethod]
		public void TestExpReadEvalFaultDivByZero()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Read("result", Exp.Build(Exp.Div(Exp.IntBin(BIN_INT), Exp.Val(0))),
					ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "integer division by zero");
			AssertEvalTrace(ae, "div", 1, ["div"]);
		}

		[TestMethod]
		public void TestExpReadCdtOutOfBoundsSubcode()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Read("result", Exp.Build(CdtOobExp()), ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.SubCode);
			AssertMessageContains(ae, "out of bounds");
			AssertEvalTrace(ae, "call", 1, ["call"]);
		}

		[TestMethod]
		public void TestExpReadAbsentBin()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Read("result", Exp.Build(Exp.IntBin(BIN_MISSING)), ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "expression references an absent bin or key");
			AssertEvalTrace(ae, "bin", 1, ["bin"]);
		}

		[TestMethod]
		public void TestExpReadWrongTypeBinReadsAbsent()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Read("result", Exp.Build(Exp.IntBin(BIN_FLOAT)), ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "expression references an absent bin or key");
			AssertEvalTrace(ae, "bin", 1, ["bin"]);
		}

		[TestMethod]
		public void TestExpReadUnknownLiteralReadsAbsent()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Read("result", Exp.Build(Exp.Unknown()), ExpReadFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "expression references an absent bin or key");
			AssertEvalTrace(ae, "unknown", 1, ["unknown"]);
		}

		[TestMethod]
		public void TestExpReadEvalNoFailSwallowsAbsent()
		{
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			Record record = client.Operate(writePolicy, stdKey,
				ExpOperation.Read("result", Exp.Build(Exp.IntBin(BIN_MISSING)),
					ExpReadFlags.EVAL_NO_FAIL));

			Assert.IsNotNull(record);
		}

		[TestMethod]
		public void TestExpReadEvalNoFailSwallowsFault()
		{
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			Record record = client.Operate(writePolicy, stdKey,
				ExpOperation.Read("result", Exp.Build(Exp.Div(Exp.IntBin(BIN_INT), Exp.Val(0))),
					ExpReadFlags.EVAL_NO_FAIL));

			Assert.IsNotNull(record);
		}

		[TestMethod]
		public void TestExpWriteEvalFaultDivByZero()
		{
			ReseedScratch();

			AerospikeException ae = ExpectOperateError(scratchKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Write("wb", Exp.Build(Exp.Div(Exp.IntBin(BIN_INT), Exp.Val(0))),
					ExpWriteFlags.DEFAULT));

			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "integer division by zero");
			AssertEvalTrace(ae, "div", 1, ["div"]);
		}

		[TestMethod]
		public void TestExpWriteCdtOutOfBoundsSubcode()
		{
			AerospikeException ae = ExpectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Write("wb", Exp.Build(CdtOobExp()), ExpWriteFlags.DEFAULT));

			Assert.AreEqual(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.SubCode);
			AssertMessageContains(ae, "out of bounds");
			AssertEvalTrace(ae, "call", 1, ["call"]);
		}

		[TestMethod]
		public void TestExpWriteCreateOnlyExistingBinNoDetails()
		{
			ReseedScratch();

			AerospikeException ae = ExpectOperateError(scratchKey, 3, ResultCode.BIN_EXISTS_ERROR,
				ExpOperation.Write(BIN_INT, Exp.Build(Exp.Add(Exp.IntBin(BIN_INT), Exp.Val(1))),
					ExpWriteFlags.CREATE_ONLY));

			AssertNoDetails(ae, ResultCode.BIN_EXISTS_ERROR);
		}

		[TestMethod]
		public void TestExpWriteUpdateOnlyMissingBinNoDetails()
		{
			ReseedScratch();

			AerospikeException ae = ExpectOperateError(scratchKey, 3, ResultCode.BIN_NOT_FOUND,
				ExpOperation.Write(BIN_MISSING, Exp.Build(Exp.Val(1)), ExpWriteFlags.UPDATE_ONLY));

			AssertNoDetails(ae, ResultCode.BIN_NOT_FOUND);
		}

		[TestMethod]
		public void TestExpWriteNilWithoutAllowDeleteNoDetails()
		{
			ReseedScratch();

			AerospikeException ae = ExpectOperateError(scratchKey, 3, ResultCode.OP_NOT_APPLICABLE,
				ExpOperation.Write(BIN_INT, Exp.Build(Exp.Nil()), ExpWriteFlags.DEFAULT));

			AssertNoDetails(ae, ResultCode.OP_NOT_APPLICABLE);
		}

		[TestMethod]
		public void TestExpWriteNilAllowDeleteDeletesBin()
		{
			ReseedScratch();
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			client.Operate(writePolicy, scratchKey,
				ExpOperation.Write(BIN_INT, Exp.Build(Exp.Nil()), ExpWriteFlags.ALLOW_DELETE));

			Record record = client.Get(null, scratchKey);
			Assert.IsNotNull(record);
			Assert.IsNull(record.GetValue(BIN_INT), "Expected bin to be deleted");
			Assert.IsNotNull(record.GetValue(SCRATCH_KEEP_BIN), "Expected untouched bin to remain");
		}

		[TestMethod]
		public void TestExpWritePolicyNoFailSwallowsViolation()
		{
			ReseedScratch();
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			client.Operate(writePolicy, scratchKey,
				ExpOperation.Write(BIN_INT, Exp.Build(Exp.Add(Exp.IntBin(BIN_INT), Exp.Val(1))),
					ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL));

			Record record = client.Get(null, scratchKey);
			Assert.IsNotNull(record);
			Assert.AreEqual(10, record.GetInt(BIN_INT));
		}

		[TestMethod]
		public void TestExpWriteEvalNoFailSwallowsFault()
		{
			ReseedScratch();
			WritePolicy writePolicy = new()
			{
				errorDetailVerbosity = 3
			};

			Record record = client.Operate(writePolicy, scratchKey,
				ExpOperation.Write(BIN_INT, Exp.Build(Exp.Div(Exp.IntBin(BIN_INT), Exp.Val(0))),
					ExpWriteFlags.EVAL_NO_FAIL));

			Assert.IsNotNull(record);
		}

		// Filter-decision explainer: outcome (key 7) and decisive operands (key 13).

		[TestMethod]
		public void TestExplainerCleanFalseWithOperands()
		{
			// BIN_INT is 10; compare against 11 so the expression is valid but false.
			Expression expression = Exp.Build(Exp.EQ(Exp.IntBin(BIN_INT), Exp.Val(11)));
			AerospikeException ae = ExpectFilteredGet(3, expression, ResultCode.FILTERED_OUT);

			ExpressionTrace trace = ae.ExpTrace;
			Assert.IsNotNull(trace);
			Assert.AreEqual(ExpressionTrace.PHASE_EVAL, trace.Phase);
			Assert.AreEqual(ExpressionTrace.OUTCOME_FALSE, trace.Outcome);

			// Operands are optional because the server drops them first when the
			// error-detail byte budget is tight.
			if (trace.Operands != null)
			{
				CollectionAssert.AreEqual(new[] { "10", "11" }, trace.Operands);
			}
		}

		[TestMethod]
		public void TestExplainerAbsentOutcomeHasNoOperands()
		{
			Expression expression = Exp.Build(Exp.EQ(Exp.IntBin(BIN_MISSING), Exp.Val(2)));
			AerospikeException ae = ExpectFilteredGet(3, expression, ResultCode.FILTERED_OUT);

			ExpressionTrace trace = ae.ExpTrace;
			Assert.IsNotNull(trace);
			Assert.AreEqual(ExpressionTrace.PHASE_EVAL, trace.Phase);
			Assert.AreEqual(ExpressionTrace.OUTCOME_ABSENT, trace.Outcome);
			Assert.IsNull(trace.Operands);
		}

		// Multi-record paths use different field walks than single-record commands.

		[TestMethod]
		public void TestQueryFilterBuildFailureTrace()
		{
			QueryPolicy policy = new()
			{
				errorDetailVerbosity = 3,
				filterExp = BuildErrorExp()
			};
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = SuiteHelpers.set
			};

			AerospikeException caught = Assert.Throws<AerospikeException>(() =>
			{
				using RecordSet records = client.Query(policy, stmt);

				while (records.Next())
				{
					// Drain because the failure can surface when the stream advances.
				}
			});

			Assert.AreEqual(ResultCode.PARAMETER_ERROR, caught.Result);
			AssertMessageContains(caught, "invalid filter expression in query");

			ExpressionTrace trace = AssertBuildTrace(caught);
			// A query filters many records per request, so the server does not
			// include the per-record outcome explainer on this build trace.
			Assert.AreEqual(-1, trace.Outcome);
			Assert.IsNull(trace.Operands);
		}

		[TestMethod]
		public void TestBatchRowFilterBuildFailureTrace()
		{
			BatchPolicy policy = new()
			{
				errorDetailVerbosity = 3,
				respondAllKeys = true
			};

			// A per-row filter failure is returned on that row. A batch-wide filter
			// build failure aborts the entire batch before individual rows are returned.
			BatchReadPolicy rowPolicy = new()
			{
				filterExp = BuildErrorExp()
			};
			BatchRead errorRow = new(rowPolicy, stdKey, true);
			BatchRead successRow = new(scratchKey, true);
			List<BatchRead> records = [errorRow, successRow];

			// respondAllKeys reports the failure on the row instead of throwing.
			client.Get(policy, records);

			Assert.AreEqual(ResultCode.PARAMETER_ERROR, errorRow.resultCode);
			Assert.IsNotNull(errorRow.serverMessage);
			StringAssert.Contains(errorRow.serverMessage, "invalid filter expression in batch request");
			Assert.IsNotNull(errorRow.expTrace);
			Assert.AreEqual(ExpressionTrace.PHASE_BUILD, errorRow.expTrace.Phase);
			Assert.IsTrue(errorRow.expTrace.ByteOffset >= 0);
			Assert.IsNull(successRow.expTrace, "Error detail must not leak between batch rows");
		}

		// Requires the runsettings test user to have the user-admin role so the suite
		// can provision a temporary principal with only the write role.
		[TestMethod]
		public void TestExplainerWriteOnlyPrincipalNoTrace()
		{
			if (!writeOnlyReady)
			{
				Assert.Inconclusive(
					"Test user must have the user-admin role to provision a write-only principal.");
			}

			WritePolicy policy = new()
			{
				errorDetailVerbosity = 3,
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin(BIN_INT), Exp.Val(99999))),
				failOnFilteredOut = true
			};

			AerospikeException ae = null;

			try
			{
				writeOnlyClient.Put(policy, stdKey, new Bin(BIN_INT, 99));
			}
			catch (AerospikeException e)
			{
				ae = e;
			}

			Assert.IsNotNull(ae);
			Assert.AreEqual(ResultCode.FILTERED_OUT, ae.Result);
			Assert.AreEqual(SubCode.NONE, ae.SubCode);
			AssertMessageContains(ae, "filtered out");
			Assert.IsNull(ae.ExpTrace, "Write-only principal must not receive filter explainer trace");
		}

		[TestMethod]
		public void TestExecuteFilterExpFilteredOutNoDetail()
		{
			WritePolicy policy = new()
			{
				errorDetailVerbosity = 3,
				filterExp = Exp.Build(Exp.EQ(Exp.IntBin(BIN_INT), Exp.Val(99999))),
				failOnFilteredOut = true
			};

			AerospikeException ae = null;

			try
			{
				client.Execute(policy, stdKey, "record_example", "writeBin", Value.Get(BIN_INT), Value.Get(99));
			}
			catch (AerospikeException e)
			{
				ae = e;
			}

			Assert.IsNotNull(ae);
			AssertNoDetails(ae, ResultCode.FILTERED_OUT);
		}
	}
}
