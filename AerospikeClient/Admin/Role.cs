/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Role definition.
	/// </summary>
	public sealed class Role
	{
		/// <summary>
		/// Manage users their roles.
		/// </summary>
		public const string UserAdmin = "user-admin";

		/// <summary>
		/// Manage server configuration.
		/// </summary>
		public const string SysAdmin = "sys-admin";

		/// <summary>
		/// Manage user defined functions and indicies.
		/// </summary>
		public const string DataAdmin = "data-admin";

		/// <summary>
		/// Manage user defined functions.
		/// </summary>
		public const string UDFAdmin = "udf-admin";

		/// <summary>
		/// Manage indicies.
		/// </summary>
		public const string SIndexAdmin = "sindex-admin";

		/// <summary>
		/// Allow read commands.
		/// </summary>
		public const string Read = "read";

		/// <summary>
		/// Allow read and write commands.
		/// </summary>
		public const string ReadWrite = "read-write";

		/// <summary>
		/// Allow read and write commands within user defined functions.
		/// </summary>
		public const string ReadWriteUdf = "read-write-udf";

		/// <summary>
		/// Allow write commands.
		/// </summary>
		public const string Write = "write";

		/// <summary>
		/// Allow truncate.
		/// </summary>
		public const string Truncate = "truncate";

		/// <summary>
		/// Role name.
		/// </summary>
		public string name;

		/// <summary>
		/// List of assigned privileges.
		/// </summary>
		public List<Privilege> privileges;

		/// <summary>
		/// List of allowable IP addresses.
		/// </summary>
		public List<string> whitelist;

		/// <summary>
		/// Maximum reads per second limit.
		/// </summary>
		public int readQuota;

		/// <summary>
		/// Maximum writes per second limit.
		/// </summary>
		public int writeQuota;

		/// <summary>
		/// Is role pre-defined.
		/// </summary>
		public bool isPredefined()
		{
			return name.Equals(ReadWrite) || name.Equals(ReadWriteUdf) || name.Equals(Read)
				|| name.Equals(SysAdmin) || name.Equals(UserAdmin) || name.Equals(DataAdmin)
				|| name.Equals(UDFAdmin) || name.Equals(SIndexAdmin)
				|| name.Equals(Write) || name.Equals(Truncate);
		}
		
		public override string ToString()
		{
			return "Role [name=" + name + ", privileges=" + privileges + ", whitelist=" + whitelist + ", readQuota=" + readQuota + ", writeQuota=" + writeQuota + "]";
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (this.GetType() != obj.GetType())
			{
				return false;
			}
			Role other = (Role)obj;
			if (name == null)
			{
				if (other.name != null)
				{
					return false;
				}
			}
			else if (!name.Equals(other.name))
			{
				return false;
			}
			return true;
		}
	}
}
