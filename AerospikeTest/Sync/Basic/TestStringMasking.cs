/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	/// <summary>
	/// Integration tests for string operations applied to bins protected by a
	/// server-side masking rule.
	/// </summary>
	/// <remarks>
	/// Each test exercises one privilege boundary:
	/// <list type="bullet">
	/// <item><description>read with the <c>read-masked</c> privilege should observe the real value;</description></item>
	/// <item><description>read without it should observe the masked value;</description></item>
	/// <item><description>modify without <c>write-masked</c> should fail with <see cref="ResultCode.ROLE_VIOLATION"/>.</description></item>
	/// </list>
	/// </remarks>
	[TestClass]
	public class TestStringMasking : TestSync
	{
		private static readonly string MASKED_BIN = "pii";
		private static readonly string UNMASKED_BIN = "public";
		private static readonly string INITIAL_VALUE = "hello world";
		private static readonly string INITIAL_PUBLIC = "visible data";
		private static readonly string MASK_FUNCTION = "redact";

		private static readonly string PRIV_USER = "stringops_reader";
		private static readonly string UNPRIV_USER = "stringops_user";
		private static readonly string USER_PASSWORD = "stringops_pw1!";

		private static readonly Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "stringmask-key");
		private static readonly StringPolicy policy = StringPolicy.Default;

		private static bool enabled;
		private static IAerospikeClient privClient;
		private static IAerospikeClient unprivClient;

		[ClassInitialize]
		public static void SetupUsersAndRule(TestContext testContext)
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "string operations");
			if (SuiteHelpers.user == null || string.IsNullOrEmpty(SuiteHelpers.user)
				|| SuiteHelpers.password == null || string.IsNullOrEmpty(SuiteHelpers.password))
			{
				Assert.Inconclusive("Skipping: admin credentials not provided");
			}

			// Probe the cluster for security; bail out cleanly if it isn't enabled.
			try
			{
				client.QueryRoles(new AdminPolicy());
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.SECURITY_NOT_ENABLED
					|| e.Result == ResultCode.SECURITY_NOT_SUPPORTED
					|| e.Result == ResultCode.NOT_AUTHENTICATED)
				{
					Assert.Inconclusive("Skipping: security not enabled on cluster");
				}
				throw;
			}

			DropUserQuiet(PRIV_USER);
			DropUserQuiet(UNPRIV_USER);

			AdminPolicy ap = new AdminPolicy();
			client.CreateUser(ap, PRIV_USER, USER_PASSWORD,
				[Role.ReadWrite, Role.ReadMasked]);
			client.CreateUser(ap, UNPRIV_USER, USER_PASSWORD,
				[Role.ReadWrite]);

			privClient = NewClient(PRIV_USER);
			unprivClient = NewClient(UNPRIV_USER);

			ApplyMaskRule(MASKED_BIN, MASK_FUNCTION, null);
			enabled = true;
		}

		[ClassCleanup]
		public static void TearDown()
		{
			if (!enabled)
			{
				return;
			}
			RemoveMaskRule(MASKED_BIN);

			try
			{
				AdminPolicy ap = new AdminPolicy();
				DropUserQuiet(PRIV_USER);
				DropUserQuiet(UNPRIV_USER);
				// If queryRoles fires (it can race role propagation), let close still run.
				client.QueryRoles(ap);
			}
			catch (Exception)
			{
			}
			finally
			{
				CloseQuiet(privClient);
				CloseQuiet(unprivClient);
			}
		}

		[TestInitialize]
		public void ResetRecord()
		{
			client.Delete(null, key);
			client.Put(null, key,
				new Bin(MASKED_BIN, INITIAL_VALUE),
				new Bin(UNMASKED_BIN, INITIAL_PUBLIC));
		}

		//=================================================================
		// Read ops: privilege gates which value the caller observes
		//=================================================================

		[TestMethod]
		public void ReadMaskedSeesRealValue_Strlen()
		{
			Record r = privClient.Operate(null, key, StringOperation.Strlen(MASKED_BIN));
			Assert.AreEqual(INITIAL_VALUE.Length, r.GetLong(MASKED_BIN));
		}

		[TestMethod]
		public void ReadMaskedSeesRealValue_Substr()
		{
			Record r = privClient.Operate(null, key, StringOperation.Substr(MASKED_BIN, 0, 5));
			Assert.AreEqual("hello", r.GetString(MASKED_BIN));
		}

		[TestMethod]
		public void UnprivilegedSeesMaskedSubstring()
		{
			Record r = unprivClient.Operate(null, key, StringOperation.Substr(MASKED_BIN, 0, 5));
			// A full-redact rule should never let the underlying characters leak.
			String value = r.GetString(MASKED_BIN);
			Assert.AreEqual(5, value.Length);
			Assert.AreNotEqual("hello", value);
		}

		[TestMethod]
		public void UnprivilegedFindOnMaskedBinDoesNotLocateRealContent()
		{
			Record r = unprivClient.Operate(null, key, StringOperation.Find(MASKED_BIN, "world"));
			Assert.AreEqual(-1L, r.GetLong(MASKED_BIN));
		}

		[TestMethod]
		public void UnprivilegedContainsOnMaskedBinIsFalse()
		{
			Record r = unprivClient.Operate(null, key, StringOperation.Contains(MASKED_BIN, "hello"));
			Assert.IsFalse(r.GetBool(MASKED_BIN));
		}

		[TestMethod]
		public void UnprivilegedStartsEndsOnMaskedBinAreFalse()
		{
			Record sw = unprivClient.Operate(null, key, StringOperation.StartsWith(MASKED_BIN, "hello"));
			Record ew = unprivClient.Operate(null, key, StringOperation.EndsWith(MASKED_BIN, "world"));
			Assert.IsFalse(sw.GetBool(MASKED_BIN));
			Assert.IsFalse(ew.GetBool(MASKED_BIN));
		}

		[TestMethod]
		public void UnprivilegedRegexCompareOnMaskedBinDoesNotMatchReal()
		{
			Record r = unprivClient.Operate(null, key, StringOperation.RegexCompare(MASKED_BIN, "hello.*"));
			Assert.IsFalse(r.GetBool(MASKED_BIN));
		}

		[TestMethod]
		public void StrlenIsUnaffectedByRedaction()
		{
			// Redact preserves length, so both clients agree on strlen/byteLength.
			Record priv = privClient.Operate(null, key, StringOperation.ByteLength(MASKED_BIN));
			Record unp = unprivClient.Operate(null, key, StringOperation.ByteLength(MASKED_BIN));
			Assert.AreEqual(INITIAL_VALUE.Length, priv.GetLong(MASKED_BIN));
			Assert.AreEqual(INITIAL_VALUE.Length, unp.GetLong(MASKED_BIN));
		}

		//=================================================================
		// Read ops on the unmasked bin — both users see the real data
		//=================================================================

		[TestMethod]
		public void UnmaskedBinIsTransparentToBothUsers()
		{
			Record priv = privClient.Operate(null, key, StringOperation.Strlen(UNMASKED_BIN));
			Record unp = unprivClient.Operate(null, key, StringOperation.Strlen(UNMASKED_BIN));
			Assert.AreEqual(INITIAL_PUBLIC.Length, priv.GetLong(UNMASKED_BIN));
			Assert.AreEqual(INITIAL_PUBLIC.Length, unp.GetLong(UNMASKED_BIN));
		}

		//=================================================================
		// Modify ops: blocked without write-masked
		//=================================================================

		[TestMethod]
		public void WriteMaskedRequired_Upper()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.Upper(policy, MASKED_BIN)));
		}

		[TestMethod]
		public void WriteMaskedRequired_Insert()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.Insert(policy, MASKED_BIN, 5, " beautiful")));
		}

		[TestMethod]
		public void WriteMaskedRequired_Concat()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.Concat(policy, MASKED_BIN, "!")));
		}

		[TestMethod]
		public void WriteMaskedRequired_Replace()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.Replace(policy, MASKED_BIN, "world", "earth")));
		}

		[TestMethod]
		public void WriteMaskedRequired_Trim()
		{
			client.Put(null, key, new Bin(MASKED_BIN, "  padded  "));
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.Trim(policy, MASKED_BIN)));
		}

		[TestMethod]
		public void WriteMaskedRequired_PadStart()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.PadStart(policy, MASKED_BIN, 20, "*")));
		}

		[TestMethod]
		public void WriteMaskedRequired_RegexReplace()
		{
			AssertRoleViolation(() => unprivClient.Operate(null, key,
				StringOperation.RegexReplace(policy, MASKED_BIN, "[0-9]+", "NUM", 0)));
		}

		//=================================================================
		// Read-masked still cannot modify; admin still can.
		//=================================================================

		[TestMethod]
		public void ReadMaskedCannotModify()
		{
			AssertRoleViolation(() => privClient.Operate(null, key,
				StringOperation.Upper(policy, MASKED_BIN)));
		}

		[TestMethod]
		public void AdminModifyOnMaskedBinSucceeds()
		{
			client.Operate(null, key, StringOperation.Upper(policy, MASKED_BIN));
			Record r = client.Get(null, key);
			Assert.AreEqual("HELLO WORLD", r.GetString(MASKED_BIN));
		}

		//=================================================================
		// Modify on unmasked bin succeeds for unprivileged user.
		//=================================================================

		[TestMethod]
		public void UnprivilegedCanModifyUnmaskedBin()
		{
			unprivClient.Operate(null, key, StringOperation.Upper(policy, UNMASKED_BIN));
			Record r = client.Get(null, key);
			Assert.AreEqual("VISIBLE DATA", r.GetString(UNMASKED_BIN));
			// The masked bin is left untouched.
			Assert.AreEqual(INITIAL_VALUE, r.GetString(MASKED_BIN));
		}

		//=================================================================
		// Constant-mask variant: unprivileged sees a fixed string
		//=================================================================

		[TestMethod]
		public void ConstantMaskIsObservedByUnprivilegedRead()
		{
			const string constBin = "secret";
			const string constValue = "HIDDEN";
			const string real = "real secret data";
			Key key = new Key(SuiteHelpers.ns, SuiteHelpers.set, "stringmask-const");

			ApplyMaskRule(constBin, "constant", "value=" + constValue);
			try
			{
				client.Delete(null, key);
				client.Put(null, key, new Bin(constBin, real));

				Record priv = privClient.Operate(null, key, StringOperation.Strlen(constBin));
				Record unp = unprivClient.Operate(null, key, StringOperation.Strlen(constBin));
				Assert.AreEqual(real.Length, priv.GetLong(constBin));
				Assert.AreEqual(constValue.Length, unp.GetLong(constBin));

				Record privSub = privClient.Operate(null, key, StringOperation.Substr(constBin, 0, 4));
				Record unpSub = unprivClient.Operate(null, key, StringOperation.Substr(constBin, 0, 4));
				Assert.AreEqual("real", privSub.GetString(constBin));
				Assert.AreEqual("HIDD", unpSub.GetString(constBin));
			}
			finally
			{
				client.Delete(null, key);
				RemoveMaskRule(constBin);
			}
		}

		//=================================================================
		// Helpers
		//=================================================================

		private delegate void OperateCall();
		private static void AssertRoleViolation(OperateCall call)
		{
			try
			{
				call();
				Assert.Fail("Expected ROLE_VIOLATION");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.ROLE_VIOLATION, e.Result);
			}
		}

		private static AerospikeClient NewClient(string user)
		{
			ClientPolicy p = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate,
				user = user,
				password = USER_PASSWORD
			};
			return new AerospikeClient(p, Host.ParseHosts(SuiteHelpers.hosts[0].name, SuiteHelpers.tlsName, SuiteHelpers.port));
		}

		private static void CloseQuiet(IAerospikeClient c)
		{
			if (c != null)
			{
				try
				{
					c.Close();
				}
				catch (Exception)
				{
					// Ignore
				}
			}
		}

		private static void DropUserQuiet(String user)
		{
			try
			{
				client.DropUser(new AdminPolicy(), user);
			}
			catch (AerospikeException)
			{
				// User did not exist; nothing to do.
			}
		}

		// Apply a masking rule via info command.
		// Format: masking:namespace=NS;set=SET;bin=BIN;type=string;function=FN[;extra]
		private static void ApplyMaskRule(String bin, String function, String extra)
		{
			string cmd = "masking:namespace=" + SuiteHelpers.ns
				+ ";set=" + SuiteHelpers.set
				+ ";bin=" + bin
				+ ";type=string;function=" + function;
			if (extra != null && !string.IsNullOrEmpty(extra))
			{
				cmd += ";" + extra;
			}
			InfoOnAllNodes(cmd);
		}

		private static void RemoveMaskRule(string bin)
		{
			string cmd = "masking:namespace=" + SuiteHelpers.ns
				+ ";set=" + SuiteHelpers.set
				+ ";bin=" + bin
				+ ";type=string;function=remove";
			InfoOnAllNodes(cmd);
		}

		private static void InfoOnAllNodes(string cmd)
		{
			foreach (Node node in client.Nodes)
			{
				Info.Request(null, node, cmd);
			}
			// Give the rule time to propagate before exercising it.
			Thread.Sleep(500);
		}

	}
}
