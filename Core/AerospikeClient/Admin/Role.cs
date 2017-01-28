/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
		/// Manage indicies and user defined functions.
		/// </summary>
		public const string DataAdmin = "data-admin";

		/// <summary>
		/// Allow read transactions.
		/// </summary>
		public const string Read = "read";
		
		/// <summary>
		/// Allow read and write transactions.
		/// </summary>
		public const string ReadWrite = "read-write";

		/// <summary>
		/// Allow read and write transactions within user defined functions.
		/// </summary>
		public const string ReadWriteUdf = "read-write-udf";

		/// <summary>
		/// Role name.
		/// </summary>
		public string name;

		/// <summary>
		/// List of assigned privileges.
		/// </summary>
		public List<Privilege> privileges;

		/// <summary>
		/// Is role pre-defined.
		/// </summary>
		public bool isPredefined()
		{
			return name.Equals(ReadWrite) || name.Equals(ReadWriteUdf) || name.Equals(Read)
				|| name.Equals(SysAdmin) || name.Equals(UserAdmin) || name.Equals(DataAdmin);
		}
	}
}
