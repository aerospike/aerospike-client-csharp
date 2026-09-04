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
				User created = FindUser(TestUser);
				Assert.IsNotNull(created, "Created user should appear in QueryUsers");
				Assert.IsTrue(created.roles.Contains(Role.Read),
					"New user should start with read role");

				client.GrantRoles(policy, TestUser, [Role.Write]);
				User granted = FindUser(TestUser);
				Assert.IsTrue(granted.roles.Contains(Role.Read));
				Assert.IsTrue(granted.roles.Contains(Role.Write),
					"GrantRoles should add write without removing read");

				client.RevokeRoles(policy, TestUser, [Role.Read]);
				User revoked = FindUser(TestUser);
				Assert.IsFalse(revoked.roles.Contains(Role.Read));
				Assert.IsTrue(revoked.roles.Contains(Role.Write),
					"RevokeRoles should remove only the requested role");

				client.ChangePassword(policy, TestUser, UpdatedPassword);

				using (AerospikeClient userClient = NewClient(TestUser, UpdatedPassword))
				{
					Assert.IsNotNull(userClient.Nodes);
					Assert.IsTrue(userClient.Nodes.Length > 0);
				}

				try
				{
					using AerospikeClient staleClient = NewClient(TestUser, InitialPassword);
					_ = staleClient.Nodes;
					Assert.Fail("Expected authentication to fail with the old password");
				}
				catch (AerospikeException e)
				{
					Assert.AreEqual(ResultCode.NOT_AUTHENTICATED, e.Result);
				}
			}
			finally
			{
				DropUserQuiet(TestUser);
			}

			Assert.IsNull(FindUser(TestUser), "Dropped user should no longer appear in QueryUsers");
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

		private static User FindUser(string userName)
		{
			List<User> users = client.QueryUsers(new AdminPolicy());
			foreach (User user in users)
			{
				if (user.name == userName)
				{
					return user;
				}
			}
			return null;
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
				user = user,
				password = password,
				timeout = SuiteHelpers.timeout
			};

			if (SuiteHelpers.tlsPolicy != null)
			{
				clientPolicy.tlsPolicy = SuiteHelpers.tlsPolicy;
			}

			return new AerospikeClient(clientPolicy, SuiteHelpers.hosts);
		}
	}
}
