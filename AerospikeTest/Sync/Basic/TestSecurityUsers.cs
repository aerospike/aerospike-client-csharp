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
	public class TestSecurityUsers : TestSync
	{
		private const string TestUser = "cov_test_user";
		private const string InitialPassword = "cov_init_pwd";
		private const string UpdatedPassword = "cov_new_pwd";
		private const int RolePropagationMaxAttempts = 40;
		private const int RolePropagationDelayMs = 250;

		private static bool securityEnabled;

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			securityEnabled = TryEnableSecurity();
			if (!securityEnabled)
			{
				return;
			}

			DropUserQuiet(TestUser);
		}

		[ClassCleanup]
		public static void TearDown()
		{
			if (!securityEnabled)
			{
				return;
			}

			DropUserQuiet(TestUser);
		}

		[TestMethod]
		public void UserLifecycleGrantRevokeAndChangePassword()
		{
			RequireSecurity();

			AdminPolicy policy = new();
			client.CreateUser(policy, TestUser, InitialPassword, [Role.Read]);
			try
			{
				User created = WaitForUserRoles(TestUser, [Role.Read]);
				Assert.IsNotNull(created, "Created user should appear in QueryUser");

				client.GrantRoles(policy, TestUser, [Role.Write]);
				User granted = WaitForUserRoles(TestUser, [Role.Read, Role.Write]);
				Assert.IsTrue(granted.roles.Contains(Role.Read));
				Assert.IsTrue(granted.roles.Contains(Role.Write),
					"GrantRoles should add write without removing read");

				client.ChangePassword(policy, TestUser, UpdatedPassword);

				Key probe = new(SuiteHelpers.ns, SuiteHelpers.set, "cov_auth_probe");
				using (AerospikeClient userClient = NewClient(TestUser, UpdatedPassword))
				{
					userClient.Put(null, probe, new Bin("v", 1));
					Record record = userClient.Get(null, probe, "v");
					Assert.IsNotNull(record);
					Assert.AreEqual(1, record.GetInt("v"));
					userClient.Delete(null, probe);
				}

				client.RevokeRoles(policy, TestUser, [Role.Read]);
				User revoked = WaitForUserRoles(TestUser, [Role.Write], [Role.Read]);
				Assert.IsFalse(revoked.roles.Contains(Role.Read));
				Assert.IsTrue(revoked.roles.Contains(Role.Write),
					"RevokeRoles should remove only the requested role");
			}
			finally
			{
				DropUserQuiet(TestUser);
			}

			WaitForUserAbsent(TestUser);
		}

		private static void WaitForUserAbsent(string userName)
		{
			for (int attempt = 0; attempt < RolePropagationMaxAttempts; attempt++)
			{
				if (QueryUserIfPresent(userName) == null)
				{
					return;
				}

				Thread.Sleep(RolePropagationDelayMs);
			}

			Assert.Fail(
				$"User '{userName}' still exists after drop within "
				+ (RolePropagationMaxAttempts * RolePropagationDelayMs / 1000)
				+ "s.");
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

		/// <summary>
		/// Role grants and revokes can lag across nodes under full-suite load.
		/// </summary>
		private static User WaitForUserRoles(
			string userName,
			IList<string> expectedRoles,
			IList<string> absentRoles = null)
		{
			for (int attempt = 0; attempt < RolePropagationMaxAttempts; attempt++)
			{
				User user = QueryUserIfPresent(userName);
				if (user?.roles != null && HasExpectedRoles(user.roles, expectedRoles, absentRoles))
				{
					return user;
				}

				Thread.Sleep(RolePropagationDelayMs);
			}

			User finalUser = QueryUserIfPresent(userName);
			string expected = string.Join(", ", expectedRoles);
			string absent = absentRoles == null ? string.Empty : string.Join(", ", absentRoles);
			string actual = finalUser?.roles == null ? "null" : string.Join(", ", finalUser.roles);
			Assert.Fail(
				$"User '{userName}' roles did not converge within "
				+ (RolePropagationMaxAttempts * RolePropagationDelayMs / 1000)
				+ "s. Expected [" + expected + "], absent [" + absent + "], actual [" + actual + "].");
			return finalUser;
		}

		private static bool HasExpectedRoles(
			List<string> roles,
			IList<string> expectedRoles,
			IList<string> absentRoles)
		{
			foreach (string role in expectedRoles)
			{
				if (!roles.Contains(role))
				{
					return false;
				}
			}

			if (absentRoles != null)
			{
				foreach (string role in absentRoles)
				{
					if (roles.Contains(role))
					{
						return false;
					}
				}
			}

			return true;
		}

		private static bool TryEnableSecurity()
		{
			if (string.IsNullOrEmpty(SuiteHelpers.user) || string.IsNullOrEmpty(SuiteHelpers.password))
			{
				return false;
			}

			try
			{
				client.QueryUsers(new AdminPolicy());
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

			Host[] hosts = new Host[SuiteHelpers.client.Nodes.Length];
			for (int i = 0; i < SuiteHelpers.client.Nodes.Length; i++)
			{
				hosts[i] = SuiteHelpers.client.Nodes[i].Host;
			}

			return new AerospikeClient(clientPolicy, hosts);
		}
	}
}
