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
	public class TestSecurityRoles : TestSync
	{
		private const string TestRole = "cov_test_role";
		private const string TestRoleUser = "cov_test_role_user";
		private const string RoleUserPassword = "cov_role_pwd";
		private const int PropagationMaxAttempts = 40;
		private const int PropagationDelayMs = 250;

		private static bool securityEnabled;

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			securityEnabled = TryEnableSecurity();
			if (!securityEnabled)
			{
				return;
			}

			DropRoleQuiet(TestRole);
			DropUserQuiet(TestRoleUser);
		}

		[ClassCleanup]
		public static void TearDown()
		{
			if (!securityEnabled)
			{
				return;
			}

			DropUserQuiet(TestRoleUser);
			DropRoleQuiet(TestRole);
		}

		[TestMethod]
		public void RoleLifecyclePrivilegesWhitelistAndQuotas()
		{
			RequireSecurity();

			AdminPolicy policy = new();
			List<Privilege> readPrivileges =
			[
				new Privilege
				{
					code = PrivilegeCode.READ,
					ns = SuiteHelpers.ns
				}
			];
			List<string> whitelist = ["0.0.0.0/0"];

			try
			{
				client.CreateRole(policy, TestRole, readPrivileges, whitelist);
				Role created = WaitForRole(TestRole, readPrivileges, whitelist);
				Assert.AreEqual(TestRole, created.name);
				Assert.IsTrue(HasPrivilege(created, PrivilegeCode.READ, SuiteHelpers.ns));
				Assert.IsTrue(created.whitelist.Contains("0.0.0.0/0"));

				List<Privilege> writePrivileges =
				[
					new Privilege
					{
						code = PrivilegeCode.WRITE,
						ns = SuiteHelpers.ns
					}
				];
				client.GrantPrivileges(policy, TestRole, writePrivileges);
				Role granted = WaitForRole(
					TestRole,
					[PrivilegeCode.READ, PrivilegeCode.WRITE],
					null,
					whitelist);
				Assert.IsTrue(HasPrivilege(granted, PrivilegeCode.WRITE, SuiteHelpers.ns));

				client.RevokePrivileges(policy, TestRole, writePrivileges);
				Role revoked = WaitForRole(TestRole, readPrivileges, whitelist);
				Assert.IsFalse(HasPrivilege(revoked, PrivilegeCode.WRITE, SuiteHelpers.ns));

				List<string> updatedWhitelist = ["127.0.0.1"];
				client.SetWhitelist(policy, TestRole, updatedWhitelist);
				Role whitelistUpdated = WaitForRole(TestRole, readPrivileges, updatedWhitelist);
				Assert.IsTrue(whitelistUpdated.whitelist.Contains("127.0.0.1"));

				TrySetQuotas(policy, TestRole, readQuota: 10000, writeQuota: 5000);

				// Re-grant write and restore open whitelist before binding a user to this role.
				client.GrantPrivileges(policy, TestRole, writePrivileges);
				client.SetWhitelist(policy, TestRole, whitelist);
				WaitForRole(
					TestRole,
					[PrivilegeCode.READ, PrivilegeCode.WRITE],
					null,
					whitelist);

				client.CreateUser(policy, TestRoleUser, RoleUserPassword, [TestRole]);
				WaitForUserRoles(TestRoleUser, [TestRole]);

				Key probe = new(SuiteHelpers.ns, SuiteHelpers.set, "cov_role_probe");
				using (AerospikeClient roleClient = NewClient(TestRoleUser, RoleUserPassword))
				{
					roleClient.Put(null, probe, new Bin("v", 1));
					Record record = roleClient.Get(null, probe, "v");
					Assert.IsNotNull(record);
					Assert.AreEqual(1, record.GetInt("v"));
					roleClient.Delete(null, probe);
				}
			}
			finally
			{
				DropUserQuiet(TestRoleUser);
				DropRoleQuiet(TestRole);
			}

			WaitForRoleAbsent(TestRole);
			WaitForUserAbsent(TestRoleUser);
		}

		private static void TrySetQuotas(AdminPolicy policy, string roleName, int readQuota, int writeQuota)
		{
			try
			{
				client.SetQuotas(policy, roleName, readQuota, writeQuota);
				Role role = WaitForRoleQuotas(roleName, readQuota, writeQuota);
				Assert.AreEqual(readQuota, role.readQuota);
				Assert.AreEqual(writeQuota, role.writeQuota);
			}
			catch (AerospikeException e) when (e.Result == ResultCode.QUOTAS_NOT_ENABLED)
			{
				// Quotas are optional on the server; admin paths are still exercised above.
			}
		}

		private static Role WaitForRoleQuotas(string roleName, int readQuota, int writeQuota)
		{
			for (int attempt = 0; attempt < PropagationMaxAttempts; attempt++)
			{
				Role role = QueryRoleIfPresent(roleName);
				if (role != null && role.readQuota == readQuota && role.writeQuota == writeQuota)
				{
					return role;
				}

				Thread.Sleep(PropagationDelayMs);
			}

			Role finalRole = QueryRoleIfPresent(roleName);
			Assert.Fail(
				$"Role '{roleName}' quotas did not converge within "
				+ (PropagationMaxAttempts * PropagationDelayMs / 1000)
				+ $"s. Expected read={readQuota}, write={writeQuota}, "
				+ $"actual read={finalRole?.readQuota}, write={finalRole?.writeQuota}.");
			return finalRole;
		}

		private static Role WaitForRole(
			string roleName,
			IList<Privilege> expectedPrivileges,
			IList<string> expectedWhitelist,
			IList<string> absentWhitelist = null)
		{
			for (int attempt = 0; attempt < PropagationMaxAttempts; attempt++)
			{
				Role role = QueryRoleIfPresent(roleName);
				if (role != null
					&& HasExpectedPrivileges(role, expectedPrivileges)
					&& HasExpectedWhitelist(role, expectedWhitelist, absentWhitelist))
				{
					return role;
				}

				Thread.Sleep(PropagationDelayMs);
			}

			Role finalRole = QueryRoleIfPresent(roleName);
			Assert.Fail(
				$"Role '{roleName}' did not converge within "
				+ (PropagationMaxAttempts * PropagationDelayMs / 1000)
				+ "s.");
			return finalRole;
		}

		private static Role WaitForRole(
			string roleName,
			IList<PrivilegeCode> expectedCodes,
			IList<PrivilegeCode> absentCodes,
			IList<string> expectedWhitelist)
		{
			List<Privilege> expected = [];
			foreach (PrivilegeCode code in expectedCodes)
			{
				expected.Add(new Privilege { code = code, ns = SuiteHelpers.ns });
			}

			Role role = WaitForRole(roleName, expected, expectedWhitelist);

			if (absentCodes != null)
			{
				foreach (PrivilegeCode code in absentCodes)
				{
					Assert.IsFalse(HasPrivilege(role, code, SuiteHelpers.ns));
				}
			}

			return role;
		}

		private static bool HasExpectedPrivileges(Role role, IList<Privilege> expectedPrivileges)
		{
			foreach (Privilege expected in expectedPrivileges)
			{
				if (!HasPrivilege(role, expected.code, expected.ns))
				{
					return false;
				}
			}

			return true;
		}

		private static bool HasPrivilege(Role role, PrivilegeCode code, string ns)
		{
			if (role.privileges == null)
			{
				return false;
			}

			foreach (Privilege privilege in role.privileges)
			{
				if (privilege.code == code
					&& (ns == null || string.Equals(privilege.ns, ns, StringComparison.Ordinal)))
				{
					return true;
				}
			}

			return false;
		}

		private static bool HasExpectedWhitelist(
			Role role,
			IList<string> expectedWhitelist,
			IList<string> absentWhitelist)
		{
			if (expectedWhitelist != null)
			{
				foreach (string entry in expectedWhitelist)
				{
					if (role.whitelist == null || !role.whitelist.Contains(entry))
					{
						return false;
					}
				}
			}

			if (absentWhitelist != null)
			{
				foreach (string entry in absentWhitelist)
				{
					if (role.whitelist != null && role.whitelist.Contains(entry))
					{
						return false;
					}
				}
			}

			return true;
		}

		private static void WaitForRoleAbsent(string roleName)
		{
			for (int attempt = 0; attempt < PropagationMaxAttempts; attempt++)
			{
				if (QueryRoleIfPresent(roleName) == null)
				{
					return;
				}

				Thread.Sleep(PropagationDelayMs);
			}

			Assert.Fail($"Role '{roleName}' still exists after drop.");
		}

		private static void WaitForUserAbsent(string userName)
		{
			for (int attempt = 0; attempt < PropagationMaxAttempts; attempt++)
			{
				if (QueryUserIfPresent(userName) == null)
				{
					return;
				}

				Thread.Sleep(PropagationDelayMs);
			}

			Assert.Fail($"User '{userName}' still exists after drop.");
		}

		private static User WaitForUserRoles(string userName, IList<string> expectedRoles)
		{
			for (int attempt = 0; attempt < PropagationMaxAttempts; attempt++)
			{
				User user = QueryUserIfPresent(userName);
				if (user?.roles != null && HasExpectedUserRoles(user.roles, expectedRoles))
				{
					return user;
				}

				Thread.Sleep(PropagationDelayMs);
			}

			Assert.Fail($"User '{userName}' roles did not converge within timeout.");
			return null;
		}

		private static bool HasExpectedUserRoles(List<string> roles, IList<string> expectedRoles)
		{
			foreach (string role in expectedRoles)
			{
				if (!roles.Contains(role))
				{
					return false;
				}
			}

			return true;
		}

		private static Role QueryRoleIfPresent(string roleName)
		{
			try
			{
				return client.QueryRole(new AdminPolicy(), roleName);
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.INVALID_ROLE)
				{
					return null;
				}

				throw;
			}
		}

		private static User QueryUserIfPresent(string userName)
		{
			try
			{
				return client.QueryUser(new AdminPolicy(), userName);
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.INVALID_USER)
				{
					return null;
				}

				throw;
			}
		}

		private static bool TryEnableSecurity()
		{
			if (string.IsNullOrEmpty(SuiteHelpers.user) || string.IsNullOrEmpty(SuiteHelpers.password))
			{
				return false;
			}

			try
			{
				client.QueryRoles(new AdminPolicy());
				return true;
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.SECURITY_NOT_ENABLED
					|| e.Result == ResultCode.SECURITY_NOT_SUPPORTED
					|| e.Result == ResultCode.NOT_AUTHENTICATED)
				{
					return false;
				}

				throw;
			}
		}

		private static void RequireSecurity()
		{
			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: security is not enabled or credentials were not provided");
			}
		}

		private static void DropRoleQuiet(string roleName)
		{
			try
			{
				client.DropRole(new AdminPolicy(), roleName);
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.INVALID_ROLE)
				{
					throw;
				}
			}
		}

		private static void DropUserQuiet(string user)
		{
			try
			{
				client.DropUser(new AdminPolicy(), user);
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.INVALID_USER)
				{
					throw;
				}
			}
		}

		private static AerospikeClient NewClient(string user, string password)
		{
			ClientPolicy clientPolicy = new()
			{
				clusterName = SuiteHelpers.clusterName,
				tlsPolicy = SuiteHelpers.tlsPolicy,
				authMode = SuiteHelpers.authMode,
				timeout = SuiteHelpers.timeout,
				useServicesAlternate = SuiteHelpers.useServicesAlternate,
				user = user,
				password = password
			};

			Host[] hosts = new Host[client.Nodes.Length];
			for (int i = 0; i < client.Nodes.Length; i++)
			{
				hosts[i] = client.Nodes[i].Host;
			}

			return new AerospikeClient(clientPolicy, hosts);
		}
	}
}
