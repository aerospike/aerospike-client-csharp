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
	public class TestQueryRoles : TestSync
	{
		private static bool securityEnabled = false;
		private static bool credentialsProvided = false;
		private static Version serverVersion = null;

		private static readonly List<string> BaseRoles = new()
		{
			Role.UserAdmin,
			Role.SysAdmin,
			Role.DataAdmin,
			Role.UDFAdmin,
			Role.SIndexAdmin,
			Role.Read,
			Role.ReadWrite,
			Role.ReadWriteUdf,
			Role.Write,
			Role.Truncate
		};

		private static readonly List<string> MaskingRoles = new()
		{
			Role.MaskingAdmin,
			Role.ReadMasked,
			Role.WriteMasked
		};

		[ClassInitialize]
		public static void Setup(TestContext testContext)
		{
			// Check credentials
			credentialsProvided = !string.IsNullOrEmpty(SuiteHelpers.user) &&
								  !string.IsNullOrEmpty(SuiteHelpers.password);

			if (!credentialsProvided)
			{
				securityEnabled = false;
				return;
			}

			// Verify security is enabled
			if (client == null)
			{
				return;
			}

			try
			{
				AdminPolicy policy = new();
				client.QueryRoles(policy);
				securityEnabled = true;

				// Get server version from a random node
				Node[] nodes = client.Nodes;
				if (nodes.Length > 0)
				{
					serverVersion = nodes[0].serverVersion;
				}
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.SECURITY_NOT_ENABLED ||
					e.Result == ResultCode.SECURITY_NOT_SUPPORTED ||
					e.Result == ResultCode.NOT_AUTHENTICATED)
				{
					securityEnabled = false;
				}
				else
				{
					throw;
				}
			}
		}

		[TestMethod]
		public void TestQueryRolesAll()
		{
			if (!credentialsProvided)
			{
				Assert.Inconclusive("Skipping test: Credentials not provided");
			}

			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: Security is not enabled on the server");
			}

			AdminPolicy policy = new();
			List<Role> roles = client.QueryRoles(policy);
			Assert.IsNotNull(roles, "Roles list should not be null");
		}

		[TestMethod]
		public void TestPreVersion8_1_1_Roles()
		{
			if (!credentialsProvided)
			{
				Assert.Inconclusive("Skipping test: Credentials not provided");
			}

			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: Security is not enabled on the server");
			}

			if (serverVersion == null || serverVersion >= Node.SERVER_VERSION_8_1_1)
			{
				Assert.Inconclusive("Skipping test: Server version is >= 8.1.1");
			}

			AdminPolicy policy = new();
			List<Role> roles = client.QueryRoles(policy);
			Assert.IsNotNull(roles, "Roles list should not be null");

			HashSet<string> roleNames = new();
			foreach (Role role in roles)
			{
				roleNames.Add(role.name);
			}

			// Verify base roles exist
			foreach (string expectedRole in BaseRoles)
			{
				Assert.IsTrue(roleNames.Contains(expectedRole),
					$"Role '{expectedRole}' should exist for server version < 8.1.1");
			}

			// Masking roles should NOT exist for server version < 8.1.1
			Assert.IsFalse(roleNames.Contains(Role.MaskingAdmin),
				$"Masking role '{Role.MaskingAdmin}' should NOT exist for server version < 8.1.1");
			Assert.IsFalse(roleNames.Contains(Role.ReadMasked),
				$"Masking role '{Role.ReadMasked}' should NOT exist for server version < 8.1.1");
			Assert.IsFalse(roleNames.Contains(Role.WriteMasked),
				$"Masking role '{Role.WriteMasked}' should NOT exist for server version < 8.1.1");
		}

		[TestMethod]
		public void TestVersion8_1_1_AndAbove_Roles()
		{
			if (!credentialsProvided)
			{
				Assert.Inconclusive("Skipping test: Credentials not provided");
			}

			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: Security is not enabled on the server");
			}

			if (serverVersion == null || serverVersion < Node.SERVER_VERSION_8_1_1)
			{
				Assert.Inconclusive("Skipping test: Server version is < 8.1.1");
			}

			AdminPolicy policy = new();
			List<Role> roles = client.QueryRoles(policy);
			Assert.IsNotNull(roles, "Roles list should not be null");

			HashSet<string> roleNames = new();
			foreach (Role role in roles)
			{
				roleNames.Add(role.name);
			}

			// Verify base roles exist
			foreach (string expectedRole in BaseRoles)
			{
				Assert.IsTrue(roleNames.Contains(expectedRole),
					$"Role '{expectedRole}' should exist for server version >= 8.1.1");
			}

			// Masking roles should exist for server version >= 8.1.1
			foreach (string maskingRole in MaskingRoles)
			{
				Assert.IsTrue(roleNames.Contains(maskingRole),
					$"Masking role '{maskingRole}' should exist for server version >= 8.1.1");
			}
		}

		[TestMethod]
		public void TestRolesByServerVersion()
		{
			if (!credentialsProvided)
			{
				Assert.Inconclusive("Skipping test: Credentials not provided");
			}

			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: Security is not enabled on the server");
			}

			AdminPolicy policy = new();
			List<Role> roles = client.QueryRoles(policy);
			Assert.IsNotNull(roles, "Roles list should not be null");

			HashSet<string> roleNames = new();
			foreach (Role role in roles)
			{
				roleNames.Add(role.name);
			}

			// Verify base roles exist for all versions
			foreach (string expectedRole in BaseRoles)
			{
				Assert.IsTrue(roleNames.Contains(expectedRole),
					$"Role '{expectedRole}' should exist for server version {serverVersion}");
			}

			if (serverVersion != null && serverVersion >= Node.SERVER_VERSION_8_1_1)
			{
				// Masking roles should exist for server version >= 8.1.1
				foreach (string maskingRole in MaskingRoles)
				{
					Assert.IsTrue(roleNames.Contains(maskingRole),
						$"Masking role '{maskingRole}' should exist for server version >= 8.1.1");
				}
			}
			else
			{
				// Masking roles should NOT exist for server version < 8.1.1
				Assert.IsFalse(roleNames.Contains(Role.MaskingAdmin),
					$"Masking role '{Role.MaskingAdmin}' should NOT exist for server version < 8.1.1");
				Assert.IsFalse(roleNames.Contains(Role.ReadMasked),
					$"Masking role '{Role.ReadMasked}' should NOT exist for server version < 8.1.1");
				Assert.IsFalse(roleNames.Contains(Role.WriteMasked),
					$"Masking role '{Role.WriteMasked}' should NOT exist for server version < 8.1.1");
			}
		}

		[TestMethod]
		public void TestPredefinedRoles()
		{
			if (!credentialsProvided)
			{
				Assert.Inconclusive("Skipping test: Credentials not provided");
			}

			if (!securityEnabled)
			{
				Assert.Inconclusive("Skipping test: Security is not enabled on the server");
			}

			AdminPolicy policy = new();
			List<Role> roles = client.QueryRoles(policy);
			Assert.IsNotNull(roles, "Roles list should not be null");

			// Verify predefined roles are marked as predefined
			foreach (Role role in roles)
			{
				if (BaseRoles.Contains(role.name) || MaskingRoles.Contains(role.name))
				{
					Assert.IsTrue(role.isPredefined(),
						$"Role '{role.name}' should be marked as predefined");
				}
			}
		}
	}
}
