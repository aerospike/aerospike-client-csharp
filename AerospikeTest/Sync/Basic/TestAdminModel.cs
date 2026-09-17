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
	public class TestAdminModel
	{
		[TestMethod]
		public void UserEqualsAndToStringUseName()
		{
			User left = new() { name = "alice", roles = ["read"] };
			User right = new() { name = "alice", roles = ["write"] };
			User different = new() { name = "bob", roles = ["read"] };

			Assert.AreEqual(left, right);
			Assert.AreNotEqual(left, different);
			Assert.AreEqual(left.GetHashCode(), right.GetHashCode());
			Assert.IsTrue(left.ToString().Contains("alice"));
			Assert.IsTrue(left.ToString().Contains("read"));
		}

		[TestMethod]
		public void PrivilegeCloneEqualsAndCodeString()
		{
			Privilege original = new()
			{
				code = PrivilegeCode.READ_WRITE,
				ns = "test",
				setName = "demo"
			};

			Privilege clone = original.Clone();
			Assert.AreEqual(original, clone);
			Assert.AreNotSame(original, clone);
			Assert.AreEqual(Role.ReadWrite, clone.CodeString);
			Assert.IsTrue(clone.CanScope());
			Assert.IsTrue(clone.ToString().Contains("test"));
			Assert.IsTrue(clone.ToString().Contains("demo"));
		}

		[TestMethod]
		public void PrivilegeCodeStringMapsAdminRoles()
		{
			Assert.AreEqual(Role.SysAdmin, CodeString(PrivilegeCode.SYS_ADMIN));
			Assert.AreEqual(Role.UserAdmin, CodeString(PrivilegeCode.USER_ADMIN));
			Assert.AreEqual(Role.DataAdmin, CodeString(PrivilegeCode.DATA_ADMIN));
			Assert.AreEqual(Role.UDFAdmin, CodeString(PrivilegeCode.UDF_ADMIN));
			Assert.AreEqual(Role.SIndexAdmin, CodeString(PrivilegeCode.SINDEX_ADMIN));
			Assert.AreEqual(Role.Read, CodeString(PrivilegeCode.READ));
			Assert.AreEqual(Role.Write, CodeString(PrivilegeCode.WRITE));
			Assert.AreEqual(Role.ReadWrite, CodeString(PrivilegeCode.READ_WRITE));
			Assert.AreEqual(Role.ReadWriteUdf, CodeString(PrivilegeCode.READ_WRITE_UDF));
			Assert.AreEqual(Role.Truncate, CodeString(PrivilegeCode.TRUNCATE));
			Assert.AreEqual(Role.MaskingAdmin, CodeString(PrivilegeCode.MASKING_ADMIN));
			Assert.AreEqual(Role.ReadMasked, CodeString(PrivilegeCode.READ_MASKED));
		}

		[TestMethod]
		public void PrivilegeCanScopeReturnsFalseForGlobalAdminCodes()
		{
			Privilege sysAdmin = new() { code = PrivilegeCode.SYS_ADMIN };
			Privilege read = new() { code = PrivilegeCode.READ, ns = "test" };

			Assert.IsFalse(sysAdmin.CanScope());
			Assert.IsTrue(read.CanScope());
		}

		private static string CodeString(PrivilegeCode code)
		{
			return new Privilege { code = code }.CodeString;
		}
	}
}
